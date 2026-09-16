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

import org.apache.celeborn.CelebornFunSuite

class RetainedShuffleLeasesSuite extends CelebornFunSuite {
  test("one replacement releasing its lease cannot revoke another replacement") {
    var now = 0L
    val leases = new RetainedShuffleLeases(() => now)
    var registered = true
    val first = leases.acquire(7, 1000L)(registered).get
    val second = leases.acquire(7, 2000L)(registered).get
    leases.release(first)
    assert(!leases.isCurrent(first))
    assert(leases.isCurrent(second))
    assert(!leases.beginRemoval(7) { registered = false })
    now = TimeUnit.MILLISECONDS.toNanos(2000L)
    assert(!leases.renew(second, 2000L))
    assert(leases.beginRemoval(7) { registered = false })
    assert(leases.acquire(7, 1000L)(registered).isEmpty)
  }

  test("renewal at the deadline cannot resurrect a token or affect a new claim") {
    var now = Long.MaxValue - TimeUnit.MILLISECONDS.toNanos(5L)
    val leases = new RetainedShuffleLeases(() => now, maxLeases = 1)
    val expired = leases.acquire(8, 10L)(true).get
    assert(leases.acquire(8, 10L)(true).isEmpty)
    // nanoTime can wrap; elapsed subtraction must still expire at the exact deadline.
    now += TimeUnit.MILLISECONDS.toNanos(10L)
    assert(!leases.isCurrent(expired))
    val replacement = leases.acquire(8, 10L)(true).get
    assert(!leases.renew(expired, 1000L))
    leases.release(expired)
    assert(leases.isCurrent(replacement))
    assert(!leases.renew(replacement.copy(shuffleId = 9), 1000L))
    leases.release(replacement.copy(shuffleId = 9))
    assert(leases.isCurrent(replacement))
  }

  test("owner shutdown fences every lease and refuses new admissions") {
    val leases = new RetainedShuffleLeases()
    val first = leases.acquire(1, 1000L)(true).get
    val second = leases.acquire(2, 1000L)(true).get
    leases.close()
    assert(!leases.isCurrent(first) && !leases.isCurrent(second))
    assert(!leases.renew(first, 1000L))
    assert(leases.acquire(1, 1000L)(true).isEmpty)
    leases.release(first)
    leases.close()
  }
}
