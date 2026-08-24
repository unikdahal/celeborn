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
import java.security.MessageDigest

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.PbRecoveryBlobPointer

/**
 * The master-side operations a task commit needs, narrowed to what the backend actually calls so
 * that the routing decision can be tested without a cluster.
 */
private[celeborn] trait RecoveryTaskCommitOps {

  /** Publishes an inline envelope and returns the canonical payload. */
  def publishInline(
      recoveryId: String,
      writeId: String,
      partitionId: Int,
      payload: Array[Byte],
      sha256: Array[Byte]): Array[Byte]

  /** Reads an inline envelope, returning null when none exists. */
  def getInline(recoveryId: String, writeId: String, partitionId: Int): Array[Byte]

  /** Publishes a pointer and returns the canonical one, which may be an earlier winner. */
  def publishPointer(
      recoveryId: String,
      writeId: String,
      partitionId: Int,
      sha256: Array[Byte],
      length: Long,
      workerIds: Seq[String]): PbRecoveryBlobPointer

  /** Reads a pointer, returning null when none exists. */
  def getPointer(recoveryId: String, writeId: String, partitionId: Int): PbRecoveryBlobPointer
}

/**
 * Chooses where a recovery task payload lives and hides that choice from the caller.
 *
 * Small payloads stay inline in replicated state, because a few hundred bytes should not pay a
 * replicated upload and an extra read. Large payloads are uploaded to a quorum of workers and
 * represented in replicated state by a pointer, which is what keeps a wide write from putting
 * gigabytes into the Raft log.
 *
 * Both routes return the same thing: the canonical payload for this partition. A caller that loses
 * arbitration receives the winner's bytes rather than its own, and never has to know which backend
 * produced them.
 */
private[celeborn] class RecoveryTaskCommitBackend(
    conf: CelebornConf,
    ops: RecoveryTaskCommitOps,
    replication: RecoveryBlobReplication,
    candidateWorkers: () => Seq[String]) extends Logging {

  private val blobEnabled = conf.recoveryBlobEnabled
  private val inlineThreshold = conf.recoveryBlobInlineThreshold
  private val formatVersion = RecoveryTaskCommitBackend.PointerFormatVersion

  /** Publishes a task payload and returns the canonical payload for this partition. */
  def publish(
      recoveryId: String,
      writeId: String,
      partitionId: Int,
      payload: Array[Byte],
      sha256: Array[Byte]): Array[Byte] = {
    require(payload != null && payload.length > 0, "A recovery task payload must not be empty")
    require(
      MessageDigest.isEqual(RecoveryBlobReplication.sha256(payload), sha256),
      "A recovery task payload must match its digest")

    if (!blobEnabled || payload.length <= inlineThreshold) {
      return ops.publishInline(recoveryId, writeId, partitionId, payload, sha256)
    }

    val candidates = candidateWorkers()
    if (candidates.isEmpty) {
      throw new IOException(
        "No worker is available to store a recovery blob; refusing to publish a pointer that " +
          "would promise durability nothing provides")
    }

    // Durable first, referenced second. A pointer must never name a payload that is not already
    // readable on a quorum of workers.
    val acknowledged = replication.upload(candidates, sha256, payload)
    val canonical = ops.publishPointer(
      recoveryId,
      writeId,
      partitionId,
      sha256,
      payload.length.toLong,
      acknowledged)

    if (MessageDigest.isEqual(canonical.getSha256.toByteArray, sha256)) {
      payload
    } else {
      // Another attempt won. Its payload is the canonical one, and this attempt's blob becomes
      // unreferenced - collection removes it once the orphan grace period has passed.
      logInfo(
        s"Recovery partition $partitionId lost blob arbitration; adopting the canonical payload")
      fetchPointer(canonical)
    }
  }

  /**
   * Reads the canonical payload for a partition, or null when none was ever published.
   *
   * Inline state is consulted first because it is authoritative and free; a pointer is only
   * meaningful when no inline record exists for the same identity.
   */
  def get(recoveryId: String, writeId: String, partitionId: Int): Array[Byte] = {
    val inline = ops.getInline(recoveryId, writeId, partitionId)
    if (inline != null) {
      return inline
    }

    val pointer = ops.getPointer(recoveryId, writeId, partitionId)
    if (pointer == null) null else fetchPointer(pointer)
  }

  private def fetchPointer(pointer: PbRecoveryBlobPointer): Array[Byte] =
    replication.fetch(
      pointer.getWorkerIdsList.toArray(Array.empty[String]).toSeq,
      pointer.getSha256.toByteArray,
      pointer.getLength)
}

private[celeborn] object RecoveryTaskCommitBackend {

  /** Version of the pointer record itself, independent of the payload's own envelope version. */
  val PointerFormatVersion = 1
}
