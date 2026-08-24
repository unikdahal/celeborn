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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite

class RecoveryBlobStoreSuite extends AnyFunSuite {

  private def withStore(body: (RecoveryBlobStore, File) => Unit): Unit = {
    val root = Files.createTempDirectory("celeborn-recovery-blobs").toFile
    try {
      body(new RecoveryBlobStore(root, 1024L * 1024L), root)
    } finally {
      deleteRecursively(root)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  private def payloadOf(text: String): Array[Byte] = text.getBytes(StandardCharsets.UTF_8)

  test("a blob round trips and its digest is verified on every read") {
    withStore { (store, _) =>
      val payload = payloadOf("task-envelope-1")
      val digest = RecoveryBlobStore.sha256(payload)

      assert(store.put(digest, payload))
      assert(store.contains(digest))
      assert(store.get(digest).sameElements(payload))
      assert(store.totalBytes === payload.length.toLong)
    }
  }

  test("storing the same content twice is idempotent, not an error") {
    withStore { (store, _) =>
      val payload = payloadOf("task-envelope-2")
      val digest = RecoveryBlobStore.sha256(payload)

      assert(store.put(digest, payload))
      // A second attempt, whether a retry or another worker's repair copy, must succeed and report
      // that nothing new was written.
      assert(!store.put(digest, payload))
      assert(store.get(digest).sameElements(payload))
    }
  }

  test("a payload that does not match its digest is refused before it reaches storage") {
    withStore { (store, _) =>
      val digest = RecoveryBlobStore.sha256(payloadOf("intended"))

      val error = intercept[IllegalArgumentException] {
        store.put(digest, payloadOf("something else"))
      }
      assert(error.getMessage.contains("does not match its digest"))
      assert(!store.contains(digest))
    }
  }

  test("a corrupt blob raises on read instead of returning bytes") {
    withStore { (store, root) =>
      val payload = payloadOf("task-envelope-3")
      val digest = RecoveryBlobStore.sha256(payload)
      store.put(digest, payload)

      val name = RecoveryBlobStore.hex(digest)
      val stored = Paths.get(
        root.getAbsolutePath,
        RecoveryBlobStore.BlobDirectory,
        name.substring(0, 2),
        s"$name${RecoveryBlobStore.BlobSuffix}")
      val bytes = Files.readAllBytes(stored)
      bytes(0) = (bytes(0) ^ 0x01).toByte
      Files.write(stored, bytes)

      val error = intercept[IOException](store.get(digest))
      assert(error.getMessage.contains("failed digest verification on read"))
    }
  }

  test("an absent blob reads as absent rather than as a failure") {
    withStore { (store, _) =>
      assert(store.get(RecoveryBlobStore.sha256(payloadOf("never stored"))) === null)
      assert(!store.contains(RecoveryBlobStore.sha256(payloadOf("never stored"))))
    }
  }

  test("digests are enumerable and deletable for repair and collection") {
    withStore { (store, _) =>
      val first = payloadOf("task-envelope-4")
      val second = payloadOf("task-envelope-5")
      val firstDigest = RecoveryBlobStore.sha256(first)
      val secondDigest = RecoveryBlobStore.sha256(second)
      store.put(firstDigest, first)
      store.put(secondDigest, second)

      assert(store.digests().toSet ===
        Set(RecoveryBlobStore.hex(firstDigest), RecoveryBlobStore.hex(secondDigest)))
      assert(store.delete(firstDigest))
      assert(!store.delete(firstDigest))
      assert(store.digests() === Seq(RecoveryBlobStore.hex(secondDigest)))
    }
  }

  test("blob size bounds and digest length are enforced") {
    withStore { (store, _) =>
      val payload = payloadOf("task-envelope-6")
      intercept[IllegalArgumentException] {
        store.put(Array.emptyByteArray, payload)
      }
      intercept[IllegalArgumentException] {
        store.put(RecoveryBlobStore.sha256(Array.emptyByteArray), Array.emptyByteArray)
      }
    }
  }

  test("blobs survive a new store instance over the same directory") {
    val root = Files.createTempDirectory("celeborn-recovery-blobs-restart").toFile
    try {
      val payload = payloadOf("task-envelope-7")
      val digest = RecoveryBlobStore.sha256(payload)
      new RecoveryBlobStore(root, 1024L).put(digest, payload)

      // A worker restart must find the blobs it acknowledged before the restart.
      val reopened = new RecoveryBlobStore(root, 1024L)
      assert(reopened.contains(digest))
      assert(reopened.get(digest).sameElements(payload))
    } finally {
      deleteRecursively(root)
    }
  }
}
