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

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.{Properties, UUID}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.util.Utils

/**
 * Provider-owned lifecycle process. It holds Celeborn registration, commit metadata, heartbeats,
 * and retention leases independently of Spark drivers. A fresh service always receives a fresh
 * application identity; restarting it cannot silently attach an old descriptor to new output.
 * This initial service is not highly available and does not restore commit state after restart.
 */
private[celeborn] final class RetainedShuffleService(conf: CelebornConf) extends AutoCloseable {
  require(conf != null)

  val incarnation: String = UUID.randomUUID().toString
  val appUniqueId: String = s"retained-$incarnation"
  private val closed = new AtomicBoolean(false)
  private[celeborn] val lifecycleManager = new LifecycleManager(appUniqueId, conf)
  lifecycleManager.rpcEnv.setupEndpoint(RetainedShuffleControl.EndpointName,
    new RetainedShuffleControlEndpoint(this))

  def isLive: Boolean = !closed.get()

  def retain(shuffleId: Int, ttlMillis: Long): Option[RetainedShuffleLease] = {
    if (closed.get()) None else lifecycleManager.retainShuffle(shuffleId, ttlMillis)
  }

  def renew(lease: RetainedShuffleLease, ttlMillis: Long): Boolean = {
    !closed.get() && lifecycleManager.renewRetainedShuffle(lease, ttlMillis)
  }

  def seal(
      lease: RetainedShuffleLease,
      expectedAttempts: Vector[Int],
      reducerCount: Int): Option[RetainedShuffleSeal] = {
    if (closed.get()) None
    else lifecycleManager.retainedReadSnapshot(lease, expectedAttempts, reducerCount).map { payload =>
      RetainedShuffleSeal.create(incarnation, lease.shuffleId, reducerCount,
        expectedAttempts, payload)
    }
  }

  def release(lease: RetainedShuffleLease): Unit = {
    lifecycleManager.releaseRetainedShuffle(lease)
  }

  /** Discovery metadata only. Its presence is not proof that this incarnation is still alive. */
  def writeEndpoint(path: Path): Unit = {
    require(path != null && !closed.get(), "service must be live before advertising its endpoint")
    val properties = new Properties()
    properties.setProperty("formatVersion", "1")
    properties.setProperty("controlEndpoint", RetainedShuffleControl.EndpointName)
    properties.setProperty("applicationId", appUniqueId)
    properties.setProperty("incarnation", incarnation)
    properties.setProperty("host", lifecycleManager.getHost)
    properties.setProperty("port", lifecycleManager.getPort.toString)
    val output = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    try properties.store(output, "Celeborn retained shuffle lifecycle endpoint")
    finally output.close()
  }

  def awaitTermination(): Unit = lifecycleManager.rpcEnv.awaitTermination()

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) lifecycleManager.stop()
  }
}

/** Run separately from measured Spark attempts; terminate explicitly when retained work expires. */
object RetainedShuffleService {
  def main(args: Array[String]): Unit = {
    require(args.length == 2, "expected Celeborn properties file and new endpoint output path")
    val conf = new CelebornConf()
    Utils.loadDefaultCelebornProperties(conf, args(0))
    val service = new RetainedShuffleService(conf)
    val shutdown = new Thread("celeborn-retained-shuffle-service-shutdown") {
      override def run(): Unit = service.close()
    }
    Runtime.getRuntime.addShutdownHook(shutdown)
    try {
      service.writeEndpoint(Paths.get(args(1)))
      service.awaitTermination()
    } finally {
      service.close()
      try Runtime.getRuntime.removeShutdownHook(shutdown)
      catch { case _: IllegalStateException => () }
    }
  }
}
