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
import java.util.{Collections, HashMap}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.{PbCommittedShuffleCatalog, PbPartitionLocation, PbPartitionLocationSet, PbSnapshotMetaInfo}
import org.apache.celeborn.common.util.RecoveryTaskCommitUtils

class ApplicationLeaseSuite extends AnyFunSuite {

  private def metadata = new SingleMasterMetaManager(null, new CelebornConf())

  private def catalog(
      appId: String = "logical-app",
      shuffleId: Int = 7,
      attempt: Int = 0,
      recoveryKey: String = "query-1/stage-1") =
    PbCommittedShuffleCatalog.newBuilder()
      .setAppId(appId)
      .setShuffleId(shuffleId)
      .setAppShuffleId(3)
      .setRecoveryKey(recoveryKey)
      .setNumMappers(1)
      .setNumPartitions(1)
      .addMapperAttempts(attempt)
      .putFileGroups(
        0,
        PbPartitionLocationSet.newBuilder()
          .addLocations(PbPartitionLocation.newBuilder().setId(0).build())
          .build())
      .build()
      .toByteArray

  test("application lease transitions are monotonic and idempotent") {
    val meta = metadata
    val first = meta.updateApplicationLeaseMeta("logical-app", 0L, 1L, "driver-1", 1000L)
    assert(first.epoch() == 1L)
    assert(meta.updateApplicationLeaseMeta(
      "logical-app",
      0L,
      1L,
      "driver-1",
      1000L) == first)

    val second = meta.updateApplicationLeaseMeta("logical-app", 1L, 2L, "driver-2", 2000L)
    assert(second.epoch() == 2L)
    assert(!meta.hasValidApplicationLease("logical-app", 1L, "driver-1", 999L))
    assert(meta.hasValidApplicationLease("logical-app", 2L, "driver-2", 1999L))
    assert(!meta.hasValidApplicationLease("logical-app", 2L, "driver-2", 2000L))
  }

  test("application lease rejects stale, skipped, and conflicting transitions") {
    val meta = metadata
    meta.updateApplicationLeaseMeta("logical-app", 0L, 1L, "driver-1", 1000L)

    intercept[IllegalStateException] {
      meta.updateApplicationLeaseMeta("logical-app", 0L, 1L, "driver-2", 1000L)
    }
    intercept[IllegalArgumentException] {
      meta.updateApplicationLeaseMeta("logical-app", 1L, 3L, "driver-2", 2000L)
    }
  }

  test("application lease renewal extends expiry and fences stale owners") {
    val meta = metadata
    meta.updateApplicationLeaseMeta("logical-app", 0L, 1L, "driver-1", 1000L)

    val renewed = meta.renewApplicationLeaseMeta("logical-app", 1L, "driver-1", 2000L)
    assert(renewed.expiresAtMs() == 2000L)
    assert(meta.renewApplicationLeaseMeta("logical-app", 1L, "driver-1", 2000L) == renewed)
    intercept[IllegalArgumentException] {
      meta.renewApplicationLeaseMeta("logical-app", 1L, "driver-1", 1500L)
    }

    meta.updateApplicationLeaseMeta("logical-app", 1L, 2L, "driver-2", 3000L)
    intercept[IllegalStateException] {
      meta.renewApplicationLeaseMeta("logical-app", 1L, "driver-1", 4000L)
    }
  }

  test("application leases survive master snapshots") {
    val source = metadata
    source.updateApplicationLeaseMeta("logical-app", 0L, 1L, "driver-1", 1000L)
    val allocations = new HashMap[String, java.util.Map[String, Integer]]()
    allocations.put("worker-1", Collections.singletonMap("disk-1", 2))
    source.updateRequestSlotsMeta("logical-app-7", "host", allocations)
    val committedCatalog = catalog()
    source.updateCommittedShuffleCatalogMeta("logical-app", 7, committedCatalog)
    source.updateSourceRecoveryAnchorMeta(
      "logical-app",
      "query-1",
      "iceberg:catalog.db.table",
      "snapshot:41")
    val taskPayload = "task-envelope".getBytes("UTF-8")
    source.updateRecoveryTaskCommitMeta(
      "logical-app",
      "query-1",
      "write-1",
      0,
      taskPayload,
      MessageDigest.getInstance("SHA-256").digest(taskPayload))
    val snapshot = Files.createTempFile("celeborn-application-lease", ".snapshot")
    try {
      source.writeMetaInfoToFile(snapshot.toFile)
      val restored = metadata
      restored.restoreMetaFromFile(snapshot.toFile)

      assert(restored.hasValidApplicationLease("logical-app", 1L, "driver-1", 999L))
      assert(restored.applicationLeases.get("logical-app").expiresAtMs() == 1000L)
      assert(restored.applicationWorkers.get("logical-app") == Collections.singleton("worker-1"))
      assert(restored.committedShuffleCatalogs.get("logical-app-7").toByteArray.sameElements(
        committedCatalog))
      assert(restored.getCommittedShuffleCatalog(
        "logical-app",
        0,
        "query-1/stage-1").toByteArray.sameElements(committedCatalog))
      assert(restored.getSourceRecoveryAnchor(
        "logical-app",
        "query-1",
        "iceberg:catalog.db.table") == "snapshot:41")
      assert(restored.getRecoveryTaskCommit(
        "logical-app",
        "query-1",
        "write-1",
        0).getPayload.toByteArray.sameElements(taskPayload))
      assert(restored.recoveryTaskCommitInlineRecords() == 1L)
      assert(restored.recoveryTaskCommitInlineBytes() > taskPayload.length)
    } finally {
      Files.deleteIfExists(snapshot)
    }
  }

  test("recovery task commits are integrity checked and first-writer-wins") {
    val meta = metadata
    val first = "winner".getBytes("UTF-8")
    val second = "speculative-loser".getBytes("UTF-8")
    def digest(bytes: Array[Byte]) = MessageDigest.getInstance("SHA-256").digest(bytes)

    val published = meta.updateRecoveryTaskCommitMeta(
      "logical-app",
      "query-1",
      "write-1",
      7,
      first,
      digest(first))
    assert(published.getPayload.toByteArray.sameElements(first))
    assert(meta.updateRecoveryTaskCommitMeta(
      "logical-app",
      "query-1",
      "write-1",
      7,
      first,
      digest(first)) == published)
    assert(meta.updateRecoveryTaskCommitMeta(
      "logical-app",
      "query-1",
      "write-1",
      7,
      second,
      digest(second)) == published)
    assert(meta.getRecoveryTaskCommit(
      "logical-app",
      "query-1",
      "write-1",
      7) == published)

    intercept[IllegalArgumentException] {
      meta.updateRecoveryTaskCommitMeta(
        "logical-app",
        "query-1",
        "write-1",
        8,
        first,
        new Array[Byte](32))
    }
    intercept[IllegalArgumentException] {
      val empty = Array.empty[Byte]
      meta.updateRecoveryTaskCommitMeta(
        "logical-app",
        "query-1",
        "write-1",
        8,
        empty,
        digest(empty))
    }
    intercept[IllegalArgumentException] {
      meta.updateRecoveryTaskCommitMeta(
        "logical-app",
        "query-1",
        "write-1",
        -2,
        first,
        digest(first))
    }

    val maxUtf8Identity = "é" * (RecoveryTaskCommitUtils.MAX_IDENTITY_UTF8_BYTES / 2)
    meta.updateRecoveryTaskCommitMeta(
      "logical-app",
      maxUtf8Identity,
      "write-2",
      0,
      first,
      digest(first))
    intercept[IllegalArgumentException] {
      meta.updateRecoveryTaskCommitMeta(
        "logical-app",
        maxUtf8Identity + "é",
        "write-2",
        0,
        first,
        digest(first))
    }
    intercept[IllegalArgumentException] {
      meta.updateRecoveryTaskCommitMeta(
        "logical-app",
        "invalid-\uD800",
        "write-2",
        0,
        first,
        digest(first))
    }
  }

  test("invalid task commit snapshot leaves existing metadata intact") {
    val payload = "snapshot-value".getBytes("UTF-8")
    val digest = MessageDigest.getInstance("SHA-256").digest(payload)
    val source = metadata
    source.updateRecoveryTaskCommitMeta("app", "recovery", "write", 0, payload, digest)
    val snapshot = Files.createTempFile("celeborn-corrupt-task-commit", ".snapshot")
    try {
      source.writeMetaInfoToFile(snapshot.toFile)
      val parsed = PbSnapshotMetaInfo.parseFrom(Files.readAllBytes(snapshot))
      val key = parsed.getRecoveryTaskCommitsMap.keySet().iterator().next()
      val record = org.apache.celeborn.common.protocol.PbRecoveryTaskCommitRecord
        .parseFrom(parsed.getRecoveryTaskCommitsMap.get(key))
        .toBuilder.clearSha256().build().toByteString
      Files.write(
        snapshot,
        parsed.toBuilder.putRecoveryTaskCommits(key, record).build().toByteArray)

      val target = metadata
      target.updateSourceRecoveryAnchorMeta("live", "execution", "source", "anchor")
      intercept[java.io.IOException] {
        target.restoreMetaFromFile(snapshot.toFile)
      }
      assert(target.getSourceRecoveryAnchor("live", "execution", "source") == "anchor")
      assert(target.recoveryTaskCommits.isEmpty)
      assert(target.recoveryTaskCommitInlineBytes() == 0L)
    } finally {
      Files.deleteIfExists(snapshot)
    }
  }

  test("recovery task commit identities are unambiguous and cleaned with the application") {
    val meta = metadata
    val payload = "value".getBytes("UTF-8")
    val digest = MessageDigest.getInstance("SHA-256").digest(payload)
    meta.updateRecoveryTaskCommitMeta("ab", "c", "d", 1, payload, digest)
    meta.updateRecoveryTaskCommitMeta("a", "bc", "d", 1, payload, digest)
    meta.updateRecoveryTaskCommitMeta("a", "b", "cd", 1, payload, digest)
    assert(meta.recoveryTaskCommits.size() == 3)

    meta.updateAppLostMeta("a")
    assert(meta.recoveryTaskCommits.size() == 1)
    assert(meta.recoveryTaskCommitInlineRecords() == 1L)
    assert(meta.getRecoveryTaskCommit("ab", "c", "d", 1) != null)
  }

  test("recovery task commit aggregate bounds fail closed without corrupting accounting") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.RECOVERY_TASK_COMMIT_MAX_INLINE_RECORDS_PER_RECOVERY.key, "1")
    conf.set(CelebornConf.RECOVERY_TASK_COMMIT_MAX_INLINE_RECORDS_GLOBAL.key, "2")
    val meta = new SingleMasterMetaManager(null, conf)
    val payload = "value".getBytes("UTF-8")
    val digest = MessageDigest.getInstance("SHA-256").digest(payload)

    meta.updateRecoveryTaskCommitMeta("app", "recovery-1", "write", 0, payload, digest)
    intercept[IllegalStateException] {
      meta.updateRecoveryTaskCommitMeta("app", "recovery-1", "write", 1, payload, digest)
    }
    assert(meta.recoveryTaskCommitInlineRecords() == 1L)
    assert(meta.recoveryTaskCommits.size() == 1)

    meta.updateRecoveryTaskCommitMeta("app", "recovery-2", "write", 0, payload, digest)
    intercept[IllegalStateException] {
      meta.updateRecoveryTaskCommitMeta("other", "recovery-3", "write", 0, payload, digest)
    }
    assert(meta.recoveryTaskCommitInlineRecords() == 2L)
    assert(meta.recoveryTaskCommits.size() == 2)
    assert(meta.recoveryTaskCommitRecoveryBuckets() == 2)

    meta.updateAppLostMeta("app")
    assert(meta.recoveryTaskCommitInlineRecords() == 0L)
    assert(meta.recoveryTaskCommitInlineBytes() == 0L)
    assert(meta.recoveryTaskCommitRecoveryBuckets() == 0)
  }

  test("source recovery anchors are immutable per logical execution and source") {
    val meta = metadata
    assert(meta.updateSourceRecoveryAnchorMeta(
      "logical-app",
      "query-1",
      "iceberg:catalog.db.table",
      "snapshot:41") == "snapshot:41")
    assert(meta.updateSourceRecoveryAnchorMeta(
      "logical-app",
      "query-1",
      "iceberg:catalog.db.table",
      "snapshot:42") == "snapshot:41")
    assert(meta.updateSourceRecoveryAnchorMeta(
      "logical-app",
      "query-1",
      "iceberg:catalog.db.other",
      "snapshot:9") == "snapshot:9")
    assert(meta.updateSourceRecoveryAnchorMeta(
      "logical-app",
      "query-2",
      "iceberg:catalog.db.table",
      "snapshot:42") == "snapshot:42")

    intercept[IllegalArgumentException] {
      meta.updateSourceRecoveryAnchorMeta("logical-app", "", "source", "anchor")
    }
  }

  test("committed shuffle catalogs are validated, immutable, and replayable") {
    val meta = metadata
    val first = catalog()
    meta.updateCommittedShuffleCatalogMeta("logical-app", 7, first)
    meta.updateCommittedShuffleCatalogMeta("logical-app", 7, first)

    intercept[IllegalStateException] {
      meta.updateCommittedShuffleCatalogMeta("logical-app", 7, catalog(attempt = 1))
    }
    intercept[IllegalArgumentException] {
      meta.updateCommittedShuffleCatalogMeta("other-app", 7, first)
    }
    intercept[IllegalArgumentException] {
      meta.updateCommittedShuffleCatalogMeta("logical-app", 7, Array[Byte](1, 2, 3))
    }
    intercept[IllegalStateException] {
      meta.updateCommittedShuffleCatalogMeta(
        "logical-app",
        10,
        catalog(shuffleId = 10))
    }

    val missingReducer = PbCommittedShuffleCatalog.newBuilder()
      .setAppId("logical-app")
      .setShuffleId(8)
      .setNumMappers(1)
      .setNumPartitions(1)
      .addMapperAttempts(0)
      .build()
      .toByteArray
    intercept[IllegalArgumentException] {
      meta.updateCommittedShuffleCatalogMeta("logical-app", 8, missingReducer)
    }

    val wrongReducer = PbCommittedShuffleCatalog.newBuilder()
      .setAppId("logical-app")
      .setShuffleId(9)
      .setNumMappers(1)
      .setNumPartitions(1)
      .addMapperAttempts(0)
      .putFileGroups(
        0,
        PbPartitionLocationSet.newBuilder()
          .addLocations(PbPartitionLocation.newBuilder().setId(1).build())
          .build())
      .build()
      .toByteArray
    intercept[IllegalArgumentException] {
      meta.updateCommittedShuffleCatalogMeta("logical-app", 9, wrongReducer)
    }
  }

  test("application loss removes committed catalogs and their recovery index") {
    val meta = metadata
    meta.updateCommittedShuffleCatalogMeta("logical-app", 7, catalog())
    meta.updateSourceRecoveryAnchorMeta("logical-app", "query-1", "source", "anchor")

    meta.updateAppLostMeta("logical-app")

    assert(meta.committedShuffleCatalogs.isEmpty)
    assert(meta.committedShuffleCatalogIndex.isEmpty)
    assert(meta.sourceRecoveryAnchors.isEmpty)
    assert(meta.getCommittedShuffleCatalog("logical-app", 0, "query-1/stage-1") == null)
  }
}
