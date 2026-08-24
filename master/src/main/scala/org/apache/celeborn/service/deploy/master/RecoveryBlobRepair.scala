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

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.PbRecoveryBlobPointer

/**
 * One payload that has fallen below its replication factor, and how to restore it.
 *
 * `source` is a replica that is still alive and can serve the bytes; `targets` are workers that
 * should receive a copy. Both are decided against the live worker set rather than the pointer's
 * recorded locations, because a pointer records where a payload was put, not where it still is.
 */
private[master] case class RecoveryBlobRepairTask(
    pointer: PbRecoveryBlobPointer,
    source: String,
    targets: Seq[String])

/**
 * Decides which recovery payloads need re-replication.
 *
 * Repair never changes what a payload is - the digest and length are fixed by the pointer - so the
 * worst outcome of a wrong decision here is wasted copying, not a corrupted recovery. The
 * interesting cases are the ones where repair must *not* act: a payload with no surviving replica
 * cannot be repaired at all, and reporting it as repairable would hide a real data loss behind an
 * endless retry.
 */
private[master] class RecoveryBlobRepairPlanner(replicationFactor: Int, maxTasksPerCycle: Int)
  extends Logging {

  require(replicationFactor > 0, "The recovery blob replication factor must be positive")
  require(maxTasksPerCycle > 0, "Repair must make progress, so at least one task per cycle")

  /**
   * Plans repairs for the pointers that need them.
   *
   * @param pointers every live pointer
   * @param liveWorkers workers currently able to hold blobs
   * @return at most `maxTasksPerCycle` tasks, most degraded first, so that a payload with one
   *         surviving copy is restored before one with two
   */
  def plan(
      pointers: Seq[PbRecoveryBlobPointer],
      liveWorkers: Set[String]): Seq[RecoveryBlobRepairTask] = {
    val candidates = pointers.flatMap { pointer =>
      val recorded = pointer.getWorkerIdsList.toArray(Array.empty[String]).toSeq
      val healthy = recorded.filter(liveWorkers.contains)
      val missing = replicationFactor - healthy.size
      if (missing <= 0) {
        None
      } else if (healthy.isEmpty) {
        // Nothing to copy from. This is unrecoverable for this payload, and saying so once is more
        // useful than planning a repair that can never run.
        logError(
          s"Recovery blob for partition ${pointer.getPartitionId} of write ${pointer.getWriteId} " +
            s"has no surviving replica among ${recorded.mkString(", ")}")
        None
      } else {
        val targets = liveWorkers.diff(healthy.toSet).toSeq.sorted.take(missing)
        if (targets.isEmpty) {
          // Every live worker already holds it; the cluster is simply smaller than the configured
          // replication factor. Repair cannot fix that and should not spin trying.
          None
        } else {
          Some((healthy.size, RecoveryBlobRepairTask(pointer, healthy.head, targets)))
        }
      }
    }

    candidates.sortBy(_._1).map(_._2).take(maxTasksPerCycle)
  }
}

/** Copies one payload between workers on the master's instruction. */
private[master] trait RecoveryBlobRepairExecutor {

  /**
   * Copies the payload named by `task` from its source to each target, returning the targets that
   * durably accepted it. A target that fails is simply absent from the result.
   */
  def replicate(task: RecoveryBlobRepairTask): Seq[String]
}

/**
 * Runs one repair cycle: plan, copy, then record the new replica set.
 *
 * The pointer is updated only after copies succeed, and only with workers that acknowledged, so a
 * pointer never claims a replica that does not hold the payload. Generation advances on every
 * successful repair, which is what stops a slow repair from overwriting a newer replica set.
 */
private[master] class RecoveryBlobRepairCycle(
    planner: RecoveryBlobRepairPlanner,
    executor: RecoveryBlobRepairExecutor,
    recordRepair: (PbRecoveryBlobPointer, Long, Seq[String]) => Unit) extends Logging {

  /** Returns how many pointers were repaired. */
  def run(pointers: Seq[PbRecoveryBlobPointer], liveWorkers: Set[String]): Int = {
    var repaired = 0
    planner.plan(pointers, liveWorkers).foreach { task =>
      try {
        val accepted = executor.replicate(task)
        if (accepted.nonEmpty) {
          val recorded = task.pointer.getWorkerIdsList.toArray(Array.empty[String]).toSeq
          val surviving = recorded.filter(liveWorkers.contains)
          recordRepair(
            task.pointer,
            task.pointer.getGeneration + 1L,
            (surviving ++ accepted).distinct)
          repaired += 1
        }
      } catch {
        case e: Exception =>
          // A failed repair leaves the pointer exactly as it was, so the next cycle sees the same
          // shortfall and tries again.
          logWarning(
            s"Failed to repair the recovery blob for partition ${task.pointer.getPartitionId}",
            e)
      }
    }

    repaired
  }
}
