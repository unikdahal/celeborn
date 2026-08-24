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
import java.util.concurrent.atomic.AtomicInteger

import com.google.protobuf.ByteString
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.PbRecoveryBlobPointer

class RecoveryTaskCommitBackendSuite extends AnyFunSuite {

  private val small = "small-envelope".getBytes(StandardCharsets.UTF_8)
  private val large = ("x" * 8192).getBytes(StandardCharsets.UTF_8)

  private def digest(payload: Array[Byte]): Array[Byte] = RecoveryBlobReplication.sha256(payload)

  private class FakeOps(
      var pointerWinner: Option[Array[Byte]] = None) extends RecoveryTaskCommitOps {
    val inlinePublished = new ConcurrentHashMap[Int, Array[Byte]]()
    val pointersPublished = new AtomicInteger(0)
    var inlineStored: Array[Byte] = _
    var storedPointer: PbRecoveryBlobPointer = _

    override def publishInline(
        recoveryId: String,
        writeId: String,
        partitionId: Int,
        payload: Array[Byte],
        sha256: Array[Byte]): Array[Byte] = {
      inlinePublished.put(partitionId, payload)
      payload
    }

    override def getInline(recoveryId: String, writeId: String, partitionId: Int): Array[Byte] =
      inlineStored

    override def publishPointer(
        recoveryId: String,
        writeId: String,
        partitionId: Int,
        sha256: Array[Byte],
        length: Long,
        workerIds: Seq[String]): PbRecoveryBlobPointer = {
      pointersPublished.incrementAndGet()
      val winner = pointerWinner.getOrElse(sha256)
      val winnerLength = if (pointerWinner.isDefined) length else length
      PbRecoveryBlobPointer.newBuilder()
        .setAppId("app")
        .setRecoveryId(recoveryId)
        .setWriteId(writeId)
        .setPartitionId(partitionId)
        .setSha256(ByteString.copyFrom(winner))
        .setLength(winnerLength)
        .setGeneration(0L)
        .setFormatVersion(1)
        .addAllWorkerIds(java.util.Arrays.asList(workerIds: _*))
        .setCreatedAtMs(1L)
        .build()
    }

    override def getPointer(
        recoveryId: String,
        writeId: String,
        partitionId: Int): PbRecoveryBlobPointer = storedPointer
  }

  /**
   * Models a worker's content-addressed store: a worker holds a payload per digest, not one
   * payload overall, so two attempts with different bytes coexist rather than overwriting.
   */
  private class FakeTransport extends RecoveryBlobTransport {
    val stored = new ConcurrentHashMap[String, Array[Byte]]()
    val uploads = new AtomicInteger(0)

    private def key(workerId: String, digest: Array[Byte]): String =
      s"$workerId/${RecoveryBlobReplication.hex(digest)}"

    def put(workerId: String, payload: Array[Byte]): Unit =
      stored.put(key(workerId, RecoveryBlobReplication.sha256(payload)), payload)

    override def upload(workerId: String, digest: Array[Byte], payload: Array[Byte]): Unit = {
      uploads.incrementAndGet()
      stored.put(key(workerId, digest), payload)
    }

    override def fetch(workerId: String, digest: Array[Byte]): Array[Byte] =
      stored.get(key(workerId, digest))
  }

  private def backend(
      ops: RecoveryTaskCommitOps,
      transport: RecoveryBlobTransport,
      blobEnabled: Boolean = true,
      workers: Seq[String] = Seq("worker-1", "worker-2", "worker-3")): RecoveryTaskCommitBackend = {
    val conf = new CelebornConf()
      .set(CelebornConf.RECOVERY_BLOB_ENABLED.key, blobEnabled.toString)
      .set(CelebornConf.RECOVERY_BLOB_INLINE_THRESHOLD.key, "4k")
    new RecoveryTaskCommitBackend(
      conf,
      ops,
      new RecoveryBlobReplication(transport, 3, 2),
      () => workers)
  }

  test("a small payload stays inline and never touches a worker") {
    val ops = new FakeOps()
    val transport = new FakeTransport()

    val canonical = backend(ops, transport).publish("r", "w", 0, small, digest(small))

    assert(canonical.sameElements(small))
    assert(ops.inlinePublished.containsKey(0))
    assert(transport.uploads.get() == 0, "an inline payload must not be uploaded")
    assert(ops.pointersPublished.get() == 0)
  }

  test("a large payload is uploaded to a quorum before its pointer is published") {
    val ops = new FakeOps()
    val transport = new FakeTransport()

    val canonical = backend(ops, transport).publish("r", "w", 1, large, digest(large))

    assert(canonical.sameElements(large))
    assert(transport.uploads.get() == 3, "the replication factor is satisfied before publication")
    assert(ops.pointersPublished.get() == 1)
    assert(ops.inlinePublished.isEmpty, "a large payload must not also be stored inline")
  }

  test("with the backend disabled every payload stays inline regardless of size") {
    val ops = new FakeOps()
    val transport = new FakeTransport()

    backend(ops, transport, blobEnabled = false).publish("r", "w", 2, large, digest(large))

    assert(ops.inlinePublished.containsKey(2))
    assert(transport.uploads.get() == 0)
  }

  test("a failed upload prevents pointer publication entirely") {
    val ops = new FakeOps()
    val failing = new RecoveryBlobTransport {
      override def upload(workerId: String, digest: Array[Byte], payload: Array[Byte]): Unit =
        throw new IOException(s"$workerId is full")
      override def fetch(workerId: String, digest: Array[Byte]): Array[Byte] = null
    }

    intercept[IOException] {
      backend(ops, failing).publish("r", "w", 3, large, digest(large))
    }
    // Nothing may reference a payload that never became durable.
    assert(ops.pointersPublished.get() == 0)
  }

  test("publication is refused when no worker is available") {
    val ops = new FakeOps()
    val error = intercept[IOException] {
      backend(ops, new FakeTransport(), workers = Seq.empty).publish(
        "r",
        "w",
        4,
        large,
        digest(large))
    }

    assert(error.getMessage.contains("No worker is available"))
    assert(ops.pointersPublished.get() == 0)
  }

  test("losing arbitration returns the winner's payload, not this attempt's") {
    val winner = ("y" * 8192).getBytes(StandardCharsets.UTF_8)
    val ops = new FakeOps(pointerWinner = Some(digest(winner)))
    val transport = new FakeTransport()
    // The winner's blob is already on a worker, uploaded by the attempt that won.
    transport.put("worker-1", winner)

    val canonical = backend(ops, transport).publish("r", "w", 5, large, digest(large))

    assert(canonical.sameElements(winner), "the caller must adopt the canonical payload")
    assert(!canonical.sameElements(large))
  }

  test("a payload that does not match its digest is refused before anything is published") {
    val ops = new FakeOps()
    val transport = new FakeTransport()

    intercept[IllegalArgumentException] {
      backend(ops, transport).publish("r", "w", 6, large, digest(small))
    }
    assert(transport.uploads.get() == 0)
    assert(ops.pointersPublished.get() == 0)
  }

  test("reads prefer inline state and fall back to a pointer") {
    val ops = new FakeOps()
    val transport = new FakeTransport()
    val blobs = backend(ops, transport)

    assert(blobs.get("r", "w", 7) === null, "nothing published means nothing to read")

    ops.storedPointer = PbRecoveryBlobPointer.newBuilder()
      .setAppId("app")
      .setRecoveryId("r")
      .setWriteId("w")
      .setPartitionId(7)
      .setSha256(ByteString.copyFrom(digest(large)))
      .setLength(large.length.toLong)
      .setGeneration(0L)
      .setFormatVersion(1)
      .addWorkerIds("worker-1")
      .setCreatedAtMs(1L)
      .build()
    transport.put("worker-1", large)
    assert(blobs.get("r", "w", 7).sameElements(large))

    ops.inlineStored = small
    assert(blobs.get("r", "w", 7).sameElements(small), "inline state wins when both exist")
  }

  test("a pointer whose replicas cannot serve the payload raises rather than reporting absence") {
    val ops = new FakeOps()
    val transport = new FakeTransport()
    ops.storedPointer = PbRecoveryBlobPointer.newBuilder()
      .setAppId("app")
      .setRecoveryId("r")
      .setWriteId("w")
      .setPartitionId(8)
      .setSha256(ByteString.copyFrom(digest(large)))
      .setLength(large.length.toLong)
      .setGeneration(0L)
      .setFormatVersion(1)
      .addWorkerIds("worker-9")
      .setCreatedAtMs(1L)
      .build()

    val error = intercept[IOException](backend(ops, transport).get("r", "w", 8))
    assert(error.getMessage.contains("No replica could serve"))
  }
}
