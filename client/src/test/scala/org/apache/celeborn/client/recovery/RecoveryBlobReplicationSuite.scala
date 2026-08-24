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

package org.apache.celeborn.client.recovery

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

import org.scalatest.funsuite.AnyFunSuite

class RecoveryBlobReplicationSuite extends AnyFunSuite {

  private val payload = "task-envelope".getBytes(StandardCharsets.UTF_8)
  private val digest = RecoveryBlobReplication.sha256(payload)

  /** A transport whose behaviour per worker is declared by the test. */
  private class FakeTransport(
      failingWorkers: Set[String] = Set.empty,
      absentWorkers: Set[String] = Set.empty,
      corruptWorkers: Set[String] = Set.empty) extends RecoveryBlobTransport {

    val stored = new ConcurrentHashMap[String, Array[Byte]]()

    override def upload(workerId: String, digest: Array[Byte], payload: Array[Byte]): Unit = {
      if (failingWorkers.contains(workerId)) {
        throw new IOException(s"$workerId is out of disk")
      }
      stored.put(workerId, payload)
    }

    override def fetch(workerId: String, digest: Array[Byte]): Array[Byte] = {
      if (failingWorkers.contains(workerId)) {
        throw new IOException(s"$workerId is unreachable")
      } else if (absentWorkers.contains(workerId)) {
        null
      } else if (corruptWorkers.contains(workerId)) {
        "corrupted".getBytes(StandardCharsets.UTF_8)
      } else {
        stored.get(workerId)
      }
    }
  }

  private def replication(
      transport: RecoveryBlobTransport,
      replicationFactor: Int = 3,
      quorum: Int = 2): RecoveryBlobReplication =
    new RecoveryBlobReplication(transport, replicationFactor, quorum)

  test("an upload stops once the replication factor is satisfied") {
    val transport = new FakeTransport()
    val acknowledged = replication(transport)
      .upload(Seq("worker-1", "worker-2", "worker-3", "worker-4"), digest, payload)

    assert(acknowledged === Seq("worker-1", "worker-2", "worker-3"))
    assert(!transport.stored.containsKey("worker-4"), "a fourth copy is wasted work")
  }

  test("a failing worker is skipped and the next candidate is used") {
    val transport = new FakeTransport(failingWorkers = Set("worker-2"))
    val acknowledged = replication(transport)
      .upload(Seq("worker-1", "worker-2", "worker-3", "worker-4"), digest, payload)

    assert(acknowledged === Seq("worker-1", "worker-3", "worker-4"))
  }

  test("falling short of quorum fails instead of publishing weaker durability") {
    val transport = new FakeTransport(failingWorkers = Set("worker-2", "worker-3"))
    val error = intercept[IOException] {
      replication(transport).upload(Seq("worker-1", "worker-2", "worker-3"), digest, payload)
    }

    assert(error.getMessage.contains("reached 1 of 2 required replicas"))
    assert(
      error.getMessage.contains("worker-2"),
      "the failure detail names the workers that failed")
  }

  test("reaching quorum without the full replication factor still succeeds") {
    val transport = new FakeTransport(failingWorkers = Set("worker-3"))
    val acknowledged = replication(transport)
      .upload(Seq("worker-1", "worker-2", "worker-3"), digest, payload)

    assert(acknowledged === Seq("worker-1", "worker-2"))
  }

  test("a payload that does not match its digest never reaches a worker") {
    val transport = new FakeTransport()
    intercept[IllegalArgumentException] {
      replication(transport).upload(
        Seq("worker-1"),
        digest,
        "other".getBytes(StandardCharsets.UTF_8))
    }
    assert(transport.stored.isEmpty)
  }

  test("a read fails over past absent and corrupt replicas") {
    val transport = new FakeTransport(
      absentWorkers = Set("worker-1"),
      corruptWorkers = Set("worker-2"))
    transport.stored.put("worker-3", payload)

    val fetched = replication(transport)
      .fetch(Seq("worker-1", "worker-2", "worker-3"), digest, payload.length.toLong)
    assert(fetched.sameElements(payload))
  }

  test("a replica returning the wrong length is rejected like a corrupt one") {
    val transport = new FakeTransport()
    transport.stored.put("worker-1", payload)

    val error = intercept[IOException] {
      replication(transport).fetch(Seq("worker-1"), digest, payload.length.toLong + 1L)
    }
    assert(error.getMessage.contains("length"))
  }

  test("exhausting every replica raises rather than reporting the payload as absent") {
    val transport = new FakeTransport(absentWorkers = Set("worker-1", "worker-2"))
    val error = intercept[IOException] {
      replication(transport).fetch(Seq("worker-1", "worker-2"), digest, payload.length.toLong)
    }

    // A pointer promised this payload is readable. Returning "absent" here would let a caller
    // conclude that committed work was never done.
    assert(error.getMessage.contains("No replica could serve"))
    assert(error.getMessage.contains("absent"))
  }

  test("repair targets only what is missing, and never a replica that is already healthy") {
    val blobs = replication(new FakeTransport())
    val live = Set("worker-1", "worker-3", "worker-4", "worker-5")

    assert(
      blobs.replicasToRepair(
        Seq("worker-1", "worker-2", "worker-3"),
        live,
        Seq("worker-4", "worker-5")) ===
        Seq("worker-4"),
      "one replica is dead, so exactly one replacement is needed")
    assert(
      blobs.replicasToRepair(
        Seq("worker-1", "worker-3", "worker-4"),
        live,
        Seq("worker-5")).isEmpty,
      "a healthy replica set needs no repair")
    assert(
      blobs.replicasToRepair(Seq("worker-2"), live, Seq("worker-1", "worker-3", "worker-4")) ===
        Seq("worker-1", "worker-3", "worker-4"),
      "no listed replica is live, so a full replication factor of fresh copies is required")
    assert(
      blobs.replicasToRepair(Seq("worker-1"), live, Seq("worker-3", "worker-4", "worker-5")) ===
        Seq("worker-3", "worker-4"),
      "one live replica remains, so only the shortfall is repaired, in candidate order")
  }

  test("a quorum larger than the replication factor is refused at construction") {
    val error = intercept[IllegalArgumentException] {
      new RecoveryBlobReplication(new FakeTransport(), 2, 3)
    }
    assert(error.getMessage.contains("must be within"))
  }
}
