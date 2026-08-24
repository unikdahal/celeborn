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

package org.apache.celeborn.service.deploy.worker

import java.io.{File, IOException}
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.security.MessageDigest
import java.util.UUID

import org.apache.celeborn.common.internal.Logging

/**
 * Content-addressed storage for recovery task payloads on a worker.
 *
 * A blob is named by the SHA-256 of its own bytes, which makes every operation here idempotent:
 * two attempts publishing the same payload write the same file, and a repair that copies a blob to
 * another worker cannot change what a reader sees. Nothing in this class interprets the payload —
 * it is an opaque Spark envelope.
 *
 * The durability contract a caller may rely on is narrow and deliberate: when [[put]] returns, the
 * bytes are fsynced, their digest has been re-verified from what was actually written, and the file
 * is reachable under its final name through an fsynced directory entry. A pointer to this blob may
 * be published only after that, so a published pointer can never reference an unreadable payload.
 *
 * [[get]] re-verifies the digest on every read. A corrupt replica raises rather than returning
 * bytes, because the caller can fail over to another replica but cannot detect corruption itself.
 */
private[worker] class RecoveryBlobStore(root: File, maxBlobBytes: Long) extends Logging {

  import RecoveryBlobStore._

  private val blobRoot: Path = new File(root, BlobDirectory).toPath

  Files.createDirectories(blobRoot)

  /**
   * Stores `payload` under `digest`, returning true when this call created the blob and false when
   * an identical blob was already present.
   */
  def put(digest: Array[Byte], payload: Array[Byte]): Boolean = {
    validateDigest(digest)
    require(payload != null, "Recovery blob payload must not be null")
    require(
      payload.length > 0 && payload.length <= maxBlobBytes,
      s"Recovery blob payload must contain between 1 and $maxBlobBytes bytes")
    val actual = sha256(payload)
    require(
      MessageDigest.isEqual(actual, digest),
      "Recovery blob payload does not match its digest")

    val target = blobPath(digest)
    if (Files.exists(target)) {
      return false
    }

    Files.createDirectories(target.getParent)
    val temp = target.getParent.resolve(s".tmp-${UUID.randomUUID()}")
    try {
      val channel = FileChannel.open(temp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
      try {
        channel.write(java.nio.ByteBuffer.wrap(payload))
        channel.force(true)
      } finally {
        channel.close()
      }

      // Verify what reached the filesystem rather than what we intended to write. A truncated or
      // mangled write must never be published under a digest that promises other content.
      val written = Files.readAllBytes(temp)
      if (written.length != payload.length || !MessageDigest.isEqual(sha256(written), digest)) {
        throw new IOException(s"Recovery blob ${hex(digest)} failed verification after write")
      }

      // Content addressing makes replacement harmless: any file already at this name holds these
      // exact bytes. The rename is what makes the blob visible, and only fsynced bytes are renamed.
      Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
      syncDirectory(target.getParent)
      true
    } finally {
      Files.deleteIfExists(temp)
    }
  }

  /** Returns the payload for `digest`, or null when this worker does not hold it. */
  def get(digest: Array[Byte]): Array[Byte] = {
    validateDigest(digest)
    val target = blobPath(digest)
    if (!Files.exists(target)) {
      return null
    }

    val payload = Files.readAllBytes(target)
    if (!MessageDigest.isEqual(sha256(payload), digest)) {
      logError(s"Recovery blob ${hex(digest)} is corrupt on this worker")
      throw new IOException(s"Recovery blob ${hex(digest)} failed digest verification on read")
    }

    payload
  }

  def contains(digest: Array[Byte]): Boolean = {
    validateDigest(digest)
    Files.exists(blobPath(digest))
  }

  /**
   * Deletes a blob, returning whether one was removed. Callers must tombstone the pointer that
   * references a blob before deleting it, so that no reader can observe a pointer whose payload is
   * already gone.
   */
  def delete(digest: Array[Byte]): Boolean = {
    validateDigest(digest)
    val target = blobPath(digest)
    val deleted = Files.deleteIfExists(target)
    if (deleted) {
      syncDirectory(target.getParent)
    }

    deleted
  }

  /** Every digest this worker holds, for repair and garbage collection. */
  def digests(): Seq[String] = {
    val collected = Seq.newBuilder[String]
    forEachBlob(path => collected += path.getFileName.toString.stripSuffix(BlobSuffix))
    collected.result()
  }

  def totalBytes: Long = {
    var total = 0L
    forEachBlob(path => total += Files.size(path))
    total
  }

  /**
   * Walks the blob directory without Scala collection converters, which differ between the Scala
   * versions Celeborn builds against.
   */
  private def forEachBlob(action: Path => Unit): Unit = {
    val stream = Files.walk(blobRoot)
    try {
      val paths = stream.iterator()
      while (paths.hasNext) {
        val path = paths.next()
        if (Files.isRegularFile(path) && path.getFileName.toString.endsWith(BlobSuffix)) {
          action(path)
        }
      }
    } finally {
      stream.close()
    }
  }

  private def blobPath(digest: Array[Byte]): Path = {
    val name = hex(digest)
    // Two-character fan-out keeps directory sizes manageable for a large recovery.
    blobRoot.resolve(name.substring(0, 2)).resolve(s"$name$BlobSuffix")
  }

  private def syncDirectory(directory: Path): Unit = {
    val channel = FileChannel.open(directory, StandardOpenOption.READ)
    try {
      channel.force(true)
    } catch {
      case e: IOException =>
        // Directory fsync is not supported everywhere; the blob itself is already durable.
        logWarning(s"Unable to fsync recovery blob directory $directory", e)
    } finally {
      channel.close()
    }
  }
}

private[worker] object RecoveryBlobStore {
  val BlobDirectory = "recovery-blobs"
  val BlobSuffix = ".blob"
  val DigestBytes = 32

  def sha256(payload: Array[Byte]): Array[Byte] =
    MessageDigest.getInstance("SHA-256").digest(payload)

  def hex(digest: Array[Byte]): String = {
    val builder = new StringBuilder(digest.length * 2)
    digest.foreach(value => builder.append(f"${value & 0xFF}%02x"))
    builder.toString
  }

  def validateDigest(digest: Array[Byte]): Unit = {
    require(
      digest != null && digest.length == DigestBytes,
      s"A recovery blob digest must be $DigestBytes bytes")
  }
}
