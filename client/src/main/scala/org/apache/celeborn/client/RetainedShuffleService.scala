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

import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.{Properties, UUID}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable

import org.apache.celeborn.client.RetainedShuffleControl.Reservation

/**
 * Retention and seal controls attached to Celeborn's existing standalone lifecycle daemon.
 * The daemon owns registration, heartbeats and shutdown. This attachment neither creates nor
 * stops its lifecycle manager. Each attachment gets a fresh incarnation so a service restart
 * cannot make old read descriptors current, even when the configured application ID is reused.
 */
private[celeborn] final class RetainedShuffleService(
    private[celeborn] val lifecycleManager: LifecycleManager) extends AutoCloseable {
  require(lifecycleManager != null)

  val incarnation: String = UUID.randomUUID().toString
  val appUniqueId: String = lifecycleManager.appUniqueId
  private val closed = new AtomicBoolean(false)
  // Keep tombstones until owner shutdown so retries cannot reuse retired IDs. This is a
  // bounded PoC admission table, not a durable catalog or an unbounded multi-tenant service.
  private val reservations = mutable.HashMap.empty[(UUID, Int), Reservation]
  private val retired = mutable.HashSet.empty[Int]
  private val maxReservations = 4096
  private val controlEndpoint = lifecycleManager.rpcEnv.setupEndpoint(
    RetainedShuffleControl.EndpointName, new RetainedShuffleControlEndpoint(this))

  def isLive: Boolean = !closed.get()

  def reserve(
      producer: UUID,
      appShuffleId: Int,
      numMappers: Int,
      numReducers: Int): Option[Reservation] = reservations.synchronized {
    require(producer != null && appShuffleId >= 0)
    require(numMappers > 0 && numMappers <= 65536)
    require(numReducers > 0 && numReducers <= 65536)
    if (closed.get()) None
    else {
      val key = (producer, appShuffleId)
      reservations.get(key) match {
        case Some(value) =>
          if (!retired.contains(value.shuffleId) && value.numMappers == numMappers &&
              value.numReducers == numReducers) Some(value) else None
        case None if reservations.size < maxReservations =>
          val value = Reservation(incarnation, appUniqueId, producer, appShuffleId,
            lifecycleManager.allocateRetainedShuffleId(), numMappers, numReducers,
            lifecycleManager.getUserIdentifier)
          reservations.put(key, value)
          Some(value)
        case _ => None
      }
    }
  }

  def retire(reservation: Reservation): Boolean = {
    require(reservation != null)
    val known = reservations.synchronized {
      val matches = !closed.get() &&
        reservations.get((reservation.producer, reservation.appShuffleId)).contains(reservation)
      if (matches) retired.add(reservation.shuffleId)
      matches
    }
    if (!known) false
    else {
      // Do not hold the admission lock over commit waits or unregister RPCs. Concurrent
      // retries may unregister twice; LifecycleManager's delayed cleanup is idempotent.
      lifecycleManager.unregisterAppShuffle(reservation.shuffleId, false)
      true
    }
  }

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
      RetainedShuffleSeal.create(appUniqueId, incarnation, lease.shuffleId, reducerCount,
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
    val output = Files.newOutputStream(
      path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    try properties.store(output, "Celeborn retained shuffle lifecycle endpoint")
    finally output.close()
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) lifecycleManager.rpcEnv.stop(controlEndpoint)
  }
}
