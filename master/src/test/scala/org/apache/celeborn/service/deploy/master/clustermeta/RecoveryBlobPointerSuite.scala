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

package org.apache.celeborn.service.deploy.master.clustermeta

import java.nio.file.Files
import java.security.MessageDigest
import java.util.{Arrays, Collections}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf

/**
 * Pointers to worker-replicated recovery payloads keep the payload out of replicated state while
 * keeping arbitration identical to the inline backend: first writer wins under an identity, an
 * exact replay is idempotent, and a repair may move replicas but never content.
 */
class RecoveryBlobPointerSuite extends AnyFunSuite {

  private def metadata = new SingleMasterMetaManager(null, new CelebornConf())

  private def digestOf(text: String): Array[Byte] =
    MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))

  private val replicas = Arrays.asList("worker-1", "worker-2", "worker-3")

  test("the first pointer wins and an exact replay is idempotent") {
    val meta = metadata
    val digest = digestOf("payload-a")

    val first = meta.updateRecoveryBlobPointerMeta(
      "logical-app",
      "query-1",
      "write-1",
      0,
      digest,
      4096L,
      1,
      replicas,
      100L)
    assert(first.getSha256.toByteArray.sameElements(digest))
    assert(first.getGeneration == 0L)
    assert(first.getWorkerIdsList.size() == 3)

    val replay = meta.updateRecoveryBlobPointerMeta(
      "logical-app",
      "query-1",
      "write-1",
      0,
      digest,
      4096L,
      1,
      replicas,
      200L)
    assert(replay.getCreatedAtMs == 100L, "an exact replay must not overwrite the winner")
  }

  test("a competing payload loses and learns the canonical pointer") {
    val meta = metadata
    val winner = digestOf("payload-a")
    meta.updateRecoveryBlobPointerMeta(
      "logical-app",
      "query-1",
      "write-1",
      0,
      winner,
      4096L,
      1,
      replicas,
      100L)

    // A speculative attempt uploaded different bytes. It must be told what won so it can delete
    // its own blob rather than assuming it succeeded.
    val loser = meta.updateRecoveryBlobPointerMeta(
      "logical-app",
      "query-1",
      "write-1",
      0,
      digestOf("payload-b"),
      8192L,
      1,
      replicas,
      300L)
    assert(loser.getSha256.toByteArray.sameElements(winner))
    assert(loser.getLength == 4096L)
  }

  test("pointers are keyed by the full identity") {
    val meta = metadata
    val digest = digestOf("payload-a")
    meta.updateRecoveryBlobPointerMeta(
      "logical-app",
      "query-1",
      "write-1",
      0,
      digest,
      1L,
      1,
      replicas,
      1L)

    assert(meta.getRecoveryBlobPointer("logical-app", "query-1", "write-1", 1) === null)
    assert(meta.getRecoveryBlobPointer("logical-app", "query-1", "write-2", 0) === null)
    assert(meta.getRecoveryBlobPointer("logical-app", "query-2", "write-1", 0) === null)
    assert(meta.getRecoveryBlobPointer("other-app", "query-1", "write-1", 0) === null)
    assert(meta.getRecoveryBlobPointer("logical-app", "query-1", "write-1", 0) !== null)
  }

  test("a repair moves replicas but never content, and must advance the generation") {
    val meta = metadata
    val digest = digestOf("payload-a")
    meta.updateRecoveryBlobPointerMeta(
      "logical-app",
      "query-1",
      "write-1",
      0,
      digest,
      4096L,
      1,
      replicas,
      100L)

    val repaired = meta.repairRecoveryBlobPointerMeta(
      "logical-app",
      "query-1",
      "write-1",
      0,
      1L,
      Arrays.asList("worker-2", "worker-4"))
    assert(repaired.getGeneration == 1L)
    assert(repaired.getSha256.toByteArray.sameElements(digest), "a repair must not change content")
    assert(repaired.getLength == 4096L)
    assert(repaired.getWorkerIdsList.contains("worker-4"))
    assert(!repaired.getWorkerIdsList.contains("worker-1"))

    // A repair computed against an older view of the pointer must not overwrite a newer one.
    val stale = intercept[IllegalStateException] {
      meta.repairRecoveryBlobPointerMeta(
        "logical-app",
        "query-1",
        "write-1",
        0,
        1L,
        Collections.singletonList("worker-9"))
    }
    assert(stale.getMessage.contains("does not advance"))
  }

  test("repairing a pointer that does not exist fails rather than creating one") {
    val meta = metadata
    val missing = intercept[IllegalStateException] {
      meta.repairRecoveryBlobPointerMeta(
        "logical-app",
        "query-1",
        "write-1",
        0,
        1L,
        replicas)
    }
    assert(missing.getMessage.contains("No recovery blob pointer exists"))
  }

  test("malformed pointers are refused before they reach replicated state") {
    val meta = metadata
    val digest = digestOf("payload-a")

    intercept[IllegalArgumentException] {
      meta.updateRecoveryBlobPointerMeta(
        "logical-app",
        "query-1",
        "write-1",
        0,
        Array.emptyByteArray,
        1L,
        1,
        replicas,
        1L)
    }
    intercept[IllegalArgumentException] {
      meta.updateRecoveryBlobPointerMeta(
        "logical-app",
        "query-1",
        "write-1",
        0,
        digest,
        0L,
        1,
        replicas,
        1L)
    }
    intercept[IllegalArgumentException] {
      meta.updateRecoveryBlobPointerMeta(
        "logical-app",
        "query-1",
        "write-1",
        0,
        digest,
        1L,
        0,
        replicas,
        1L)
    }
    intercept[IllegalArgumentException] {
      meta.updateRecoveryBlobPointerMeta(
        "logical-app",
        "query-1",
        "write-1",
        0,
        digest,
        1L,
        1,
        Collections.emptyList(),
        1L)
    }
    assert(meta.recoveryBlobPointers.isEmpty)
  }

  test("pointers survive a master snapshot and are dropped with their application") {
    val source = metadata
    val digest = digestOf("payload-a")
    source.updateRecoveryBlobPointerMeta(
      "logical-app",
      "query-1",
      "write-1",
      0,
      digest,
      4096L,
      1,
      replicas,
      100L)

    val snapshot = Files.createTempFile("celeborn-blob-pointer-snapshot", ".bin")
    try {
      source.writeMetaInfoToFile(snapshot.toFile)
      val restored = metadata
      restored.restoreMetaFromFile(snapshot.toFile)

      val pointer = restored.getRecoveryBlobPointer("logical-app", "query-1", "write-1", 0)
      assert(pointer !== null)
      assert(pointer.getSha256.toByteArray.sameElements(digest))
      assert(pointer.getWorkerIdsList.size() == 3)

      restored.updateAppLostMeta("logical-app")
      assert(restored.getRecoveryBlobPointer("logical-app", "query-1", "write-1", 0) === null)
      assert(restored.recoveryTaskCommitInlineRecords() == 0L)
    } finally {
      Files.deleteIfExists(snapshot)
    }
  }
}
