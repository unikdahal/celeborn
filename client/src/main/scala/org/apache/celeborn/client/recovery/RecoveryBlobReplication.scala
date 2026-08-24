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

import org.apache.celeborn.common.internal.Logging

/**
 * Moves recovery blobs between a client and the workers that store them.
 *
 * Implementations carry bytes and nothing else: identity, verification and quorum are decided by
 * [[RecoveryBlobReplication]] so that every transport gets the same guarantees.
 */
private[celeborn] trait RecoveryBlobTransport {

  /** Stores a payload on one worker, throwing if the worker did not durably accept it. */
  def upload(workerId: String, digest: Array[Byte], payload: Array[Byte]): Unit

  /** Reads a payload from one worker, returning null when that worker does not hold it. */
  def fetch(workerId: String, digest: Array[Byte]): Array[Byte]
}

/**
 * Quorum replication for recovery blobs.
 *
 * A payload becomes referenceable only after `quorum` workers have durably accepted it, because the
 * pointer published afterwards promises that the payload is readable. Falling short of quorum is a
 * failure rather than a partial success: a pointer that referenced fewer replicas than configured
 * would silently weaken the durability the operator asked for.
 *
 * Reads verify content on arrival and fail over to the next replica on corruption, which is the
 * property content addressing buys - a corrupt replica is detectable by anyone holding the digest.
 */
private[celeborn] class RecoveryBlobReplication(
    transport: RecoveryBlobTransport,
    replicationFactor: Int,
    quorum: Int) extends Logging {

  require(replicationFactor > 0, "Recovery blob replication factor must be positive")
  require(
    quorum > 0 && quorum <= replicationFactor,
    s"Recovery blob quorum $quorum must be within [1, $replicationFactor]")

  /**
   * Uploads `payload` to workers drawn from `candidates` and returns the workers that acknowledged.
   *
   * Candidates are tried in order until `replicationFactor` acknowledge. A worker that fails is
   * skipped rather than retried, because the next candidate is a better use of the attempt than the
   * one that just failed.
   */
  def upload(
      candidates: Seq[String],
      digest: Array[Byte],
      payload: Array[Byte]): Seq[String] = {
    validate(digest, payload)
    val distinct = candidates.distinct
    require(distinct.nonEmpty, "Recovery blob upload requires at least one candidate worker")

    val acknowledged = Seq.newBuilder[String]
    var accepted = 0
    val failures = Seq.newBuilder[String]
    val iterator = distinct.iterator
    while (accepted < replicationFactor && iterator.hasNext) {
      val workerId = iterator.next()
      try {
        transport.upload(workerId, digest, payload)
        acknowledged += workerId
        accepted += 1
      } catch {
        case e: Exception =>
          failures += s"$workerId: ${Option(e.getMessage).getOrElse(e.getClass.getName)}"
          logWarning(s"Worker $workerId did not accept a recovery blob", e)
      }
    }

    if (accepted < quorum) {
      throw new IOException(
        s"Recovery blob reached $accepted of $quorum required replicas; " +
          s"failures: ${failures.result().mkString("; ")}")
    }

    acknowledged.result()
  }

  /**
   * Reads a payload from the first replica that returns verifiable bytes.
   *
   * Absence and corruption are both treated as this replica being unusable, and the next one is
   * tried. Exhausting every replica is an error: the pointer promised a readable payload, so an
   * empty result would let a caller mistake committed work for work that was never done.
   */
  def fetch(locations: Seq[String], digest: Array[Byte], length: Long): Array[Byte] = {
    RecoveryBlobReplication.validateDigest(digest)
    require(locations.nonEmpty, "A recovery blob pointer must list at least one replica")

    val failures = Seq.newBuilder[String]
    locations.distinct.foreach { workerId =>
      try {
        val payload = transport.fetch(workerId, digest)
        if (payload == null) {
          failures += s"$workerId: absent"
        } else if (payload.length != length) {
          failures += s"$workerId: length ${payload.length}, expected $length"
          logWarning(s"Worker $workerId returned a recovery blob of the wrong length")
        } else if (!MessageDigest.isEqual(RecoveryBlobReplication.sha256(payload), digest)) {
          failures += s"$workerId: digest mismatch"
          logWarning(s"Worker $workerId returned a corrupt recovery blob")
        } else {
          return payload
        }
      } catch {
        case e: Exception =>
          failures += s"$workerId: ${Option(e.getMessage).getOrElse(e.getClass.getName)}"
          logWarning(s"Worker $workerId could not serve a recovery blob", e)
      }
    }

    throw new IOException(
      s"No replica could serve recovery blob ${RecoveryBlobReplication.hex(digest)}; " +
        s"attempts: ${failures.result().mkString("; ")}")
  }

  /** Workers that must receive a copy before this pointer meets its replication factor again. */
  def replicasToRepair(current: Seq[String], live: Set[String], candidates: Seq[String])
      : Seq[String] = {
    val healthy = current.filter(live.contains)
    val missing = replicationFactor - healthy.size
    if (missing <= 0) {
      Seq.empty
    } else {
      candidates.distinct.filterNot(healthy.contains).filter(live.contains).take(missing)
    }
  }

  private def validate(digest: Array[Byte], payload: Array[Byte]): Unit = {
    RecoveryBlobReplication.validateDigest(digest)
    require(payload != null && payload.length > 0, "A recovery blob payload must not be empty")
    require(
      MessageDigest.isEqual(RecoveryBlobReplication.sha256(payload), digest),
      "A recovery blob payload must match its digest")
  }
}

private[celeborn] object RecoveryBlobReplication {

  val DigestBytes = 32

  def sha256(payload: Array[Byte]): Array[Byte] =
    MessageDigest.getInstance("SHA-256").digest(payload)

  def hex(digest: Array[Byte]): String = {
    val builder = new StringBuilder(digest.length * 2)
    digest.foreach(value => builder.append(f"${value & 0xff}%02x"))
    builder.toString
  }

  def validateDigest(digest: Array[Byte]): Unit = {
    require(
      digest != null && digest.length == DigestBytes,
      s"A recovery blob digest must be $DigestBytes bytes")
  }
}
