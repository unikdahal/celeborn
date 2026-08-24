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

package org.apache.celeborn.service.deploy.master

import java.security.MessageDigest

import scala.collection.mutable

import com.google.protobuf.ByteString
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.protocol.PbRecoveryBlobPointer

class RecoveryBlobRepairSuite extends AnyFunSuite {

  private def pointer(
      partitionId: Int,
      workers: Seq[String],
      generation: Long = 0L): PbRecoveryBlobPointer =
    PbRecoveryBlobPointer.newBuilder()
      .setAppId("logical-app")
      .setRecoveryId("query-1")
      .setWriteId("write-1")
      .setPartitionId(partitionId)
      .setSha256(ByteString.copyFrom(
        MessageDigest.getInstance("SHA-256").digest(s"payload-$partitionId".getBytes("UTF-8"))))
      .setLength(4096L)
      .setGeneration(generation)
      .setFormatVersion(1)
      .addAllWorkerIds(java.util.Arrays.asList(workers: _*))
      .setCreatedAtMs(1L)
      .build()

  private def planner(replicationFactor: Int = 3, maxTasks: Int = 10) =
    new RecoveryBlobRepairPlanner(replicationFactor, maxTasks)

  test("a fully replicated payload needs no repair") {
    val tasks = planner().plan(
      Seq(pointer(0, Seq("worker-1", "worker-2", "worker-3"))),
      Set("worker-1", "worker-2", "worker-3", "worker-4"))

    assert(tasks.isEmpty)
  }

  test("a shortfall is repaired from a surviving replica onto workers that lack the payload") {
    val tasks = planner().plan(
      Seq(pointer(0, Seq("worker-1", "worker-2", "worker-3"))),
      Set("worker-1", "worker-4", "worker-5"))

    assert(tasks.size == 1)
    assert(tasks.head.source === "worker-1", "the source must be a replica that is still alive")
    assert(tasks.head.targets === Seq("worker-4", "worker-5"))
  }

  test("a payload with no surviving replica is not planned for repair") {
    // There is nothing to copy from. Planning a repair here would retry forever and hide the loss.
    val tasks = planner().plan(
      Seq(pointer(0, Seq("worker-1", "worker-2"))),
      Set("worker-3", "worker-4"))

    assert(tasks.isEmpty)
  }

  test("a cluster smaller than the replication factor does not spin") {
    val tasks = planner().plan(
      Seq(pointer(0, Seq("worker-1", "worker-2"))),
      Set("worker-1", "worker-2"))

    assert(tasks.isEmpty, "every live worker already holds it; repair cannot invent a third")
  }

  test("the most degraded payloads are repaired first") {
    val tasks = planner(maxTasks = 2).plan(
      Seq(
        pointer(0, Seq("worker-1", "worker-2")),
        pointer(1, Seq("worker-1")),
        pointer(2, Seq("worker-1", "worker-2"))),
      Set("worker-1", "worker-2", "worker-4", "worker-5"))

    assert(tasks.size == 2, "a cycle is bounded so repair cannot starve the write path")
    assert(tasks.head.pointer.getPartitionId == 1, "one surviving copy is more urgent than two")
  }

  test("a repair records only the workers that accepted the copy") {
    val recorded = mutable.Buffer.empty[(Int, Long, Seq[String])]
    val executor = new RecoveryBlobRepairExecutor {
      // worker-5 failed to take the copy, so it must not appear in the new replica set.
      override def replicate(task: RecoveryBlobRepairTask): Seq[String] = Seq("worker-4")
    }
    val cycle = new RecoveryBlobRepairCycle(
      planner(),
      executor,
      (pointer, generation, workers) =>
        recorded += ((pointer.getPartitionId, generation, workers)))

    val repaired = cycle.run(
      Seq(pointer(0, Seq("worker-1", "worker-2", "worker-3"), generation = 7L)),
      Set("worker-1", "worker-4", "worker-5"))

    assert(repaired == 1)
    assert(recorded.size == 1)
    val (partitionId, generation, workers) = recorded.head
    assert(partitionId == 0)
    assert(generation == 8L, "the generation must advance so a stale repair cannot win")
    assert(workers === Seq("worker-1", "worker-4"))
    assert(!workers.contains("worker-2"), "a dead replica is dropped from the pointer")
    assert(!workers.contains("worker-5"), "a worker that did not accept the copy is not recorded")
  }

  test("a repair that copies nothing leaves the pointer untouched") {
    val recorded = mutable.Buffer.empty[(Int, Long, Seq[String])]
    val cycle = new RecoveryBlobRepairCycle(
      planner(),
      (_: RecoveryBlobRepairTask) => Seq.empty,
      (pointer, generation, workers) =>
        recorded += ((pointer.getPartitionId, generation, workers)))

    val repaired = cycle.run(
      Seq(pointer(0, Seq("worker-1", "worker-2", "worker-3"))),
      Set("worker-1", "worker-4"))

    assert(repaired == 0)
    assert(recorded.isEmpty, "a pointer must never claim a replica that has no copy")
  }

  test("a throwing executor leaves the pointer for the next cycle") {
    val recorded = mutable.Buffer.empty[(Int, Long, Seq[String])]
    val cycle = new RecoveryBlobRepairCycle(
      planner(),
      (_: RecoveryBlobRepairTask) => throw new RuntimeException("worker unreachable"),
      (pointer, generation, workers) =>
        recorded += ((pointer.getPartitionId, generation, workers)))

    val repaired = cycle.run(
      Seq(pointer(0, Seq("worker-1", "worker-2", "worker-3"))),
      Set("worker-1", "worker-4"))

    assert(repaired == 0)
    assert(recorded.isEmpty)
  }
}
