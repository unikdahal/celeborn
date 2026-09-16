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

import java.nio.file.{Files, Paths}
import java.util.{Properties, UUID}
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.protocol.TransportModuleConstants
import org.apache.celeborn.common.rpc.RpcEnv
import org.apache.celeborn.common.util.Utils

/** Experimental driver connection to an independently owned lifecycle daemon. */
final class StandaloneShuffleProducer(endpointFile: String, conf: CelebornConf)
  extends AutoCloseable {
  require(!conf.authEnabledOnClient, "standalone lifecycle client authentication is unsupported")
  private val properties = new Properties()
  private val endpointPath = Paths.get(endpointFile)
  require(Files.size(endpointPath) <= 16384, "standalone endpoint file is too large")
  private val input = Files.newInputStream(endpointPath)
  try properties.load(input) finally input.close()
  private def property(name: String): String = {
    val value = properties.getProperty(name)
    require(value != null && value.nonEmpty, s"missing standalone endpoint property: $name")
    value
  }
  require(property("formatVersion") == "1")
  require(property("controlEndpoint") == RetainedShuffleControl.EndpointName)
  val applicationId: String = property("applicationId")
  val host: String = property("host")
  val port: Int = property("port").toInt
  private val incarnation = property("incarnation")
  require(port > 0 && port <= 65535)
  private val producer = UUID.randomUUID()
  private val rpcEnv = RpcEnv.create("retained-producer-" + producer,
    TransportModuleConstants.RPC_APP_CLIENT_MODULE, Utils.localHostName(conf), 0,
    conf, Role.CLIENT, None)
  private val control = new RetainedShuffleControlClient(rpcEnv, host, port, incarnation)
  private val registrations = mutable.HashMap.empty[Int, RetainedShuffleControl.Reservation]
  private var closed = false

  // Reservation happens before Spark dispatches any writer tasks. A retry of this call is
  // idempotent, but a new driver instance receives a new producer namespace and native IDs.
  def register(appShuffleId: Int, numMappers: Int, numReducers: Int)
      : StandaloneShuffleRegistration = synchronized {
    require(!closed, "standalone producer is closed")
    val value = control.reserve(producer, appShuffleId, numMappers, numReducers)
      .getOrElse(throw new IllegalStateException("standalone shuffle reservation rejected"))
    require(value.applicationId == applicationId, "standalone application identity changed")
    registrations.put(appShuffleId, value)
    StandaloneShuffleRegistration(value.shuffleId, value.user)
  }

  /**
   * Export committed native metadata after Spark has frozen its accepted encoded attempts.
   * On success the bounded server lease is intentionally left alive for handoff, even if this
   * producer exits. It expires without renewal; a replacement must obtain its own read lease.
   */
  def publish(
      appShuffleId: Int,
      acceptedEncodedAttempts: Array[Int],
      handoffTtlMillis: Long): Array[Byte] = synchronized {
    require(!closed && acceptedEncodedAttempts != null)
    val reservation = registrations.getOrElse(appShuffleId,
      throw new IllegalArgumentException("shuffle has no standalone producer reservation"))
    val attempts = acceptedEncodedAttempts.toVector
    require(attempts.size == reservation.numMappers && attempts.forall(_ >= 0))
    val lease = RetainedShuffleLeaseHandle.acquire(control, reservation.shuffleId, handoffTtlMillis)
      .getOrElse(throw new IllegalStateException("shuffle could not be retained for publication"))
    var published = false
    try {
      // MapperEnd acknowledges before the asynchronous StageEnd commits worker files.
      // Wait on this publication caller, never on the retention RPC endpoint. Every probe
      // still requires the exact accepted attempts and a live lease; a mismatch never seals.
      val waitMillis = math.min(60000L, math.min(conf.clientPushStageEndTimeout,
        handoffTtlMillis / 2L))
      val started = System.nanoTime()
      var sealedOutput = control.seal(lease.lease, attempts, reservation.numReducers)
      while (sealedOutput.isEmpty && lease.isCurrent &&
          System.nanoTime() - started < TimeUnit.MILLISECONDS.toNanos(waitMillis)) {
        Thread.sleep(math.min(100L, math.max(1L, waitMillis)))
        if (lease.isCurrent) {
          sealedOutput = control.seal(lease.lease, attempts, reservation.numReducers)
        }
      }
      val seal = sealedOutput.getOrElse(throw new IllegalStateException(
        "committed shuffle did not become available with Spark's accepted winners"))
      require(seal.applicationId == applicationId, "sealed application identity changed")
      val descriptor = RetainedShuffleDescriptor.encode(
        RetainedShuffleDescriptor(host, port, reservation.user, seal))
      require(lease.renew(), "publication lease expired while sealing")
      published = true
      descriptor
    } finally {
      if (!published) lease.close()
    }
  }

  def unregister(appShuffleId: Int): Unit = synchronized {
    registrations.get(appShuffleId).foreach { value =>
      require(control.retire(value), "standalone shuffle retirement rejected")
      registrations.remove(appShuffleId)
    }
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      var failure: Throwable = null
      try {
        registrations.keys.toVector.foreach { id =>
          try unregister(id)
          catch {
            case NonFatal(error) =>
              if (failure == null) failure = error else failure.addSuppressed(error)
          }
        }
      } finally {
        rpcEnv.shutdown()
      }
      if (failure != null) throw failure
    }
  }
}

/** Spark keeps its local shuffle ID; native writers use this separately allocated ID. */
final case class StandaloneShuffleRegistration(shuffleId: Int, user: UserIdentifier)
