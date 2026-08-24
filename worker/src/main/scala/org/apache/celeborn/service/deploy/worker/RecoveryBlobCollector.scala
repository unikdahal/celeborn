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

import org.apache.celeborn.common.internal.Logging

/**
 * Deletes recovery blobs that no pointer will ever name.
 *
 * A blob becomes unreferenced in two ordinary ways: a speculative attempt lost arbitration, so its
 * payload was never published, or the pointer that named it was dropped with its application.
 * Neither leaves anything on the worker to say so, which is why collection asks the master.
 *
 * Two rules keep collection from destroying live data:
 *
 *  - **Age before reference.** A blob is uploaded before its pointer exists, so a young blob is
 *    indistinguishable from an orphan. Only blobs older than the grace period are considered, and
 *    the grace period must exceed the longest upload-to-publication gap.
 *  - **Unreferenced must be an answer, not a silence.** If the master cannot be asked, the blob is
 *    kept. Deleting on a failed lookup would turn a network problem into data loss.
 */
private[worker] class RecoveryBlobCollector(
    store: RecoveryBlobStore,
    referenced: (String, Array[Byte]) => Boolean,
    orphanGraceMs: Long,
    clock: () => Long = () => System.currentTimeMillis()) extends Logging {

  require(orphanGraceMs > 0, "The recovery blob orphan grace period must be positive")

  /** Collects across every application this worker holds blobs for, returning how many it removed. */
  def collect(): Int = store.applications().map(collectApplication).sum

  /** Collects one application's orphans, returning how many blobs were removed. */
  def collectApplication(appId: String): Int = {
    val deadline = clock() - orphanGraceMs
    var removed = 0
    store.digests(appId).foreach { hex =>
      val digest = RecoveryBlobCollector.parseHex(hex)
      if (digest != null) {
        val modifiedAt = store.modifiedAtMs(appId, digest)
        if (modifiedAt >= 0L && modifiedAt < deadline) {
          try {
            if (!referenced(appId, digest)) {
              if (store.delete(appId, digest)) {
                removed += 1
                logInfo(s"Collected unreferenced recovery blob $hex for $appId")
              }
            }
          } catch {
            case e: Exception =>
              // Keep the blob. A blob that outlives its usefulness costs disk; a blob deleted
              // because a lookup failed costs a recovery.
              logWarning(s"Could not determine whether recovery blob $hex is referenced", e)
          }
        }
      }
    }

    removed
  }
}

private[worker] object RecoveryBlobCollector {

  /** Parses a stored blob name, returning null when it is not a digest this store wrote. */
  def parseHex(hex: String): Array[Byte] = {
    if (hex == null || hex.length != RecoveryBlobStore.DigestBytes * 2) {
      return null
    }

    val digest = new Array[Byte](RecoveryBlobStore.DigestBytes)
    var index = 0
    while (index < digest.length) {
      val high = Character.digit(hex.charAt(index * 2), 16)
      val low = Character.digit(hex.charAt(index * 2 + 1), 16)
      if (high < 0 || low < 0) {
        return null
      }
      digest(index) = ((high << 4) | low).toByte
      index += 1
    }

    digest
  }
}
