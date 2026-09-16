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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/** Client-side lease deadline, conservatively anchored before each control request is sent. */
private[celeborn] final class RetainedShuffleLeaseHandle private (
    control: RetainedShuffleControlClient,
    val lease: RetainedShuffleLease,
    ttlMillis: Long,
    initialStart: Long,
    nanoTime: () => Long) extends AutoCloseable {
  private val duration = TimeUnit.MILLISECONDS.toNanos(ttlMillis)
  private val started = new AtomicLong(initialStart)
  private val closed = new AtomicBoolean(false)
  private val renewing = new AtomicBoolean(false)
  private val expired = new AtomicBoolean(false)

  def isCurrent: Boolean = {
    if (nanoTime() - started.get() >= duration) expired.set(true)
    !closed.get() && !expired.get()
  }

  // No lock or remote call is taken by isCurrent while a renewal is in flight.
  def renew(): Boolean = {
    if (!isCurrent || !renewing.compareAndSet(false, true)) false
    else {
      try {
        val requestStart = nanoTime()
        if (!control.renew(lease, ttlMillis) || !isCurrent) {
          expired.set(true)
          false
        } else {
          started.set(requestStart)
          isCurrent
        }
      } finally {
        renewing.set(false)
      }
    }
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) control.release(lease)
  }
}

private[celeborn] object RetainedShuffleLeaseHandle {
  def acquire(
      control: RetainedShuffleControlClient,
      shuffleId: Int,
      ttlMillis: Long,
      nanoTime: () => Long = () => System.nanoTime()): Option[RetainedShuffleLeaseHandle] = {
    require(control != null && nanoTime != null)
    require(ttlMillis > 0 && ttlMillis <= TimeUnit.HOURS.toMillis(1))
    val start = nanoTime()
    control.acquire(shuffleId, ttlMillis).flatMap { lease =>
      val handle = new RetainedShuffleLeaseHandle(control, lease, ttlMillis, start, nanoTime)
      if (handle.isCurrent) Some(handle)
      else {
        handle.close()
        None
      }
    }
  }
}
