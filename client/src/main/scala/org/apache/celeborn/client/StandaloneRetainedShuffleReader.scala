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

import java.io.{IOException, InputStream}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.protocol.TransportModuleConstants
import org.apache.celeborn.common.rpc.RpcEnv
import org.apache.celeborn.common.util.Utils

/** Experimental native reader owned by one replacement consumer, independently of its producer. */
final class StandaloneRetainedShuffleReader(
    descriptorBytes: Array[Byte],
    ttlMillis: Long,
    conf: CelebornConf) extends AutoCloseable {
  require(!conf.authEnabledOnClient, "standalone lifecycle client authentication is unsupported")
  private val descriptor = RetainedShuffleDescriptor.decode(descriptorBytes)
  private val rpcEnv = RpcEnv.create("retained-reader-" + UUID.randomUUID(),
    TransportModuleConstants.RPC_APP_CLIENT_MODULE, Utils.localHostName(conf), 0,
    conf, Role.CLIENT, None)
  private val reader = {
    var claimed = false
    try {
      val value = RetainedShuffleReader.claim(rpcEnv, descriptor.host, descriptor.port,
        descriptor.seal, ttlMillis, conf, descriptor.user)
        .getOrElse(throw new IOException("retained shuffle is no longer claimable"))
      claimed = true
      value
    } finally {
      if (!claimed) rpcEnv.shutdown()
    }
  }
  private val closed = new AtomicBoolean(false)

  def numMappers: Int = descriptor.seal.mapperAttempts.size
  def numReducers: Int = descriptor.seal.reducerCount
  def isCurrent: Boolean = !closed.get() && reader.lease.isCurrent

  /** Call from a consumer-owned renewal worker, never from a task's fetch thread. */
  def renew(): Boolean = !closed.get() && reader.lease.renew()

  def openPartition(
      partitionId: Int,
      attemptNumber: Int,
      taskId: Long,
      startMapIndex: Int,
      endMapIndex: Int,
      metrics: MetricsCallback): InputStream = {
    reader.openPartition(partitionId, attemptNumber, taskId, startMapIndex, endMapIndex, metrics)
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try reader.close() finally rpcEnv.shutdown()
    }
  }
}
