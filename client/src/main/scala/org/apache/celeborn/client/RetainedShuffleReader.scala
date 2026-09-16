/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.client

import java.io.{FilterInputStream, IOException, InputStream}
import java.util.ArrayList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol.{MessageType, PartitionLocation}
import org.apache.celeborn.common.protocol.message.ControlMessages
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcEnv

/**
 * Native retained reads use the sealed reducer locations and mapper winners, never an unrelated
 * driver-local shuffle ID or a refreshed file-group cache. The owner must renew the lease outside
 * task fetch threads. Expiry permanently fences the handle, including already-open streams.
 */
private[celeborn] final class RetainedShuffleReader private (
    client: ShuffleClientImpl,
    val lease: RetainedShuffleLeaseHandle,
    val seal: RetainedShuffleSeal,
    metadata: GetReducerFileGroupResponse) extends AutoCloseable {
  private val closed = new AtomicBoolean(false)
  private val streams = ConcurrentHashMap.newKeySet[InputStream]()

  def openPartition(
      partitionId: Int,
      attemptNumber: Int,
      taskId: Long,
      startMapIndex: Int,
      endMapIndex: Int,
      metrics: MetricsCallback): InputStream = {
    require(partitionId >= 0 && partitionId < seal.reducerCount && attemptNumber >= 0)
    require(startMapIndex >= 0 && endMapIndex >= startMapIndex &&
      endMapIndex <= seal.mapperAttempts.size && metrics != null)
    if (closed.get() || !lease.isCurrent) {
      throw new IOException("retained read lease is not current")
    }
    val locations = new ArrayList[PartitionLocation]()
    Option(metadata.fileGroup.get(Integer.valueOf(partitionId))).foreach { group =>
      locations.addAll(group)
    }
    val input = client.readPartition(seal.shuffleId, seal.shuffleId, partitionId,
      attemptNumber, taskId, startMapIndex, endMapIndex, null, locations, null,
      null, null, seal.mapperAttempts.toArray, metrics, true)
    val guarded = new FilterInputStream(input) {
      private val streamClosed = new AtomicBoolean(false)
      private def check(): Unit = {
        if (streamClosed.get() || closed.get() || !lease.isCurrent) {
          try close() finally throw new IOException("retained shuffle stream is fenced")
        }
      }
      override def read(): Int = {
        check()
        val value = in.read()
        check()
        value
      }
      override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
        check()
        val count = in.read(bytes, offset, length)
        check()
        count
      }
      override def skip(count: Long): Long = {
        check()
        val skipped = in.skip(count)
        check()
        skipped
      }
      override def available(): Int = {
        check()
        in.available()
      }
      override def close(): Unit = {
        if (streamClosed.compareAndSet(false, true)) {
          streams.remove(this)
          in.close()
        }
      }
    }
    streams.add(guarded)
    if (closed.get() || !lease.isCurrent) {
      try guarded.close() finally throw new IOException("retained read expired during stream open")
    }
    guarded
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      var failure: Throwable = null
      def cleanup(action: => Unit): Unit = {
        try action
        catch {
          case NonFatal(error) =>
            if (failure == null) failure = error else failure.addSuppressed(error)
        }
      }
      streams.asScala.foreach(stream => cleanup(stream.close()))
      cleanup(client.shutdown())
      cleanup(lease.close())
      if (failure != null) throw failure
    }
  }
}

private[celeborn] object RetainedShuffleReader {
  def claim(
      rpcEnv: RpcEnv,
      host: String,
      port: Int,
      storedSeal: RetainedShuffleSeal,
      ttlMillis: Long,
      conf: CelebornConf,
      user: UserIdentifier): Option[RetainedShuffleReader] = {
    require(RetainedShuffleSeal.valid(storedSeal), "stored retained seal is invalid")
    val control = new RetainedShuffleControlClient(rpcEnv, host, port, storedSeal.incarnation)
    RetainedShuffleLeaseHandle.acquire(control, storedSeal.shuffleId, ttlMillis).flatMap { lease =>
      var client: ShuffleClientImpl = null
      var transferred = false
      try {
        val current = control.seal(lease.lease, storedSeal.mapperAttempts, storedSeal.reducerCount)
        if (!current.contains(storedSeal) || !lease.isCurrent) None
        else {
          val message = new TransportMessage(MessageType.GET_REDUCER_FILE_GROUP_RESPONSE,
            storedSeal.nativeReadMetadata.toArray)
          val metadata = ControlMessages.fromTransportMessage(message) match {
            case value: GetReducerFileGroupResponse => value
            case _ => throw new IOException("retained seal has no reducer file groups")
          }
          require(metadata.status == StatusCode.SUCCESS && metadata.broadcast.isEmpty &&
            metadata.pushFailedBatches.isEmpty &&
            metadata.attempts.toVector == storedSeal.mapperAttempts,
            "native retained metadata is inconsistent with its seal")
          // Dedicated ownership avoids shutting down a compute application's shared client.
          client = new ShuffleClientImpl(storedSeal.applicationId, conf, user)
          client.setupLifecycleManagerRef(host, port)
          val reader = new RetainedShuffleReader(client, lease, storedSeal, metadata)
          transferred = true
          Some(reader)
        }
      } finally {
        if (!transferred) {
          try { if (client != null) client.shutdown() }
          finally lease.close()
        }
      }
    }
  }
}
