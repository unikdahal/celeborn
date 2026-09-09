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

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.mutable

/** Attempt-independent retention token owned by a long-lived provider lifecycle service. */
private[celeborn] final case class RetainedShuffleLease(shuffleId: Int, token: UUID)

/**
 * Bounded local retention leases, fenced against lifecycle cleanup. Deadlines use elapsed time,
 * not wall time. Expired tokens cannot be renewed or resurrected. This is process-local state:
 * it does not survive loss of the provider lifecycle service and does not certify committed data.
 */
private[celeborn] final class RetainedShuffleLeases(
    nanoTime: () => Long = () => System.nanoTime(),
    maxLeases: Int = 4096,
    maxTtlMillis: Long = TimeUnit.HOURS.toMillis(1)) {
  require(nanoTime != null && maxLeases > 0)
  require(maxTtlMillis > 0 && maxTtlMillis <= TimeUnit.DAYS.toMillis(1))

  private case class Entry(lease: RetainedShuffleLease, started: Long, duration: Long)
  private val leases = mutable.HashMap.empty[UUID, Entry]
  private var closed = false

  private def expire(now: Long): Unit = {
    val expired = leases.iterator.collect {
      case (token, entry) if now - entry.started >= entry.duration => token
    }.toVector
    expired.foreach(leases.remove)
  }

  private def duration(ttlMillis: Long): Long = {
    require(ttlMillis > 0 && ttlMillis <= maxTtlMillis, "retention TTL exceeds its bound")
    TimeUnit.MILLISECONDS.toNanos(ttlMillis)
  }

  // The callbacks below must only inspect or modify the local registered-shuffle set. They must
  // never perform RPC, filesystem I/O, or wait for commit completion while this lock is held.
  def acquire(shuffleId: Int, ttlMillis: Long)(registered: => Boolean)
      : Option[RetainedShuffleLease] = synchronized {
    require(shuffleId >= 0)
    val ttl = duration(ttlMillis)
    val now = nanoTime()
    expire(now)
    if (closed || leases.size >= maxLeases || !registered) None
    else {
      val lease = RetainedShuffleLease(shuffleId, UUID.randomUUID())
      leases.put(lease.token, Entry(lease, now, ttl))
      Some(lease)
    }
  }

  def renew(lease: RetainedShuffleLease, ttlMillis: Long): Boolean = synchronized {
    val ttl = duration(ttlMillis)
    val now = nanoTime()
    expire(now)
    if (closed || lease == null) false
    else leases.get(lease.token) match {
      case Some(entry) if entry.lease == lease =>
        leases.update(lease.token, Entry(lease, now, ttl))
        true
      case _ => false
    }
  }

  def release(lease: RetainedShuffleLease): Unit = synchronized {
    if (lease != null && leases.get(lease.token).exists(_.lease == lease)) {
      leases.remove(lease.token)
    }
  }

  def beginRemoval(shuffleId: Int)(unregisterLocally: => Unit): Boolean = synchronized {
    expire(nanoTime())
    if (leases.valuesIterator.exists(_.lease.shuffleId == shuffleId)) false
    else {
      unregisterLocally
      true
    }
  }

  def close(): Unit = synchronized {
    closed = true
    leases.clear()
  }
}
