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
import java.nio.file.attribute.FileTime

import org.scalatest.funsuite.AnyFunSuite

class RecoveryBlobCollectorSuite extends AnyFunSuite {

  private val appId = "logical-app"
  private val graceMs = 60000L

  private def withStore(body: (RecoveryBlobStore, File) => Unit): Unit = {
    val root = Files.createTempDirectory("celeborn-blob-collector").toFile
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

  /** Backdates a blob so the test does not have to wait out a grace period. */
  private def age(root: File, app: String, digest: Array[Byte], byMs: Long): Unit = {
    val name = RecoveryBlobStore.hex(digest)
    val path = Paths.get(
      root.getAbsolutePath,
      RecoveryBlobStore.BlobDirectory,
      app,
      name.substring(0, 2),
      s"$name${RecoveryBlobStore.BlobSuffix}")
    Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis() - byMs))
  }

  test("an unreferenced blob past the grace period is collected") {
    withStore { (store, root) =>
      val payload = payloadOf("orphan")
      val digest = RecoveryBlobStore.sha256(payload)
      store.put(appId, digest, payload)
      age(root, appId, digest, graceMs * 2)

      val collector = new RecoveryBlobCollector(store, (_, _) => false, graceMs)
      assert(collector.collect() == 1)
      assert(!store.contains(appId, digest))
    }
  }

  test("a young blob is never collected, however unreferenced it looks") {
    withStore { (store, _) =>
      val payload = payloadOf("just-uploaded")
      val digest = RecoveryBlobStore.sha256(payload)
      store.put(appId, digest, payload)

      // A blob is uploaded before its pointer exists. During that window it is indistinguishable
      // from an orphan, and collecting it would delete a payload about to become canonical.
      val collector = new RecoveryBlobCollector(store, (_, _) => false, graceMs)
      assert(collector.collect() == 0)
      assert(store.contains(appId, digest))
    }
  }

  test("a referenced blob is kept no matter how old it is") {
    withStore { (store, root) =>
      val payload = payloadOf("still-referenced")
      val digest = RecoveryBlobStore.sha256(payload)
      store.put(appId, digest, payload)
      age(root, appId, digest, graceMs * 100)

      val collector = new RecoveryBlobCollector(store, (_, _) => true, graceMs)
      assert(collector.collect() == 0)
      assert(store.contains(appId, digest))
    }
  }

  test("a blob is kept when the reference lookup fails") {
    withStore { (store, root) =>
      val payload = payloadOf("unknown-state")
      val digest = RecoveryBlobStore.sha256(payload)
      store.put(appId, digest, payload)
      age(root, appId, digest, graceMs * 2)

      // Deleting on a failed lookup would turn a network problem into data loss.
      val collector = new RecoveryBlobCollector(
        store,
        (_, _) => throw new IOException("master unreachable"),
        graceMs)
      assert(collector.collect() == 0)
      assert(store.contains(appId, digest))
    }
  }

  test("collection is scoped per application and asks about the right one") {
    withStore { (store, root) =>
      val first = payloadOf("app-one-payload")
      val second = payloadOf("app-two-payload")
      val firstDigest = RecoveryBlobStore.sha256(first)
      val secondDigest = RecoveryBlobStore.sha256(second)
      store.put("app-one", firstDigest, first)
      store.put("app-two", secondDigest, second)
      age(root, "app-one", firstDigest, graceMs * 2)
      age(root, "app-two", secondDigest, graceMs * 2)

      val collector =
        new RecoveryBlobCollector(store, (app, _) => app == "app-two", graceMs)
      assert(collector.collect() == 1)
      assert(!store.contains("app-one", firstDigest), "app-one's orphan is collected")
      assert(store.contains("app-two", secondDigest), "app-two's referenced blob survives")
    }
  }

  test("a file that is not a digest is ignored rather than deleted") {
    withStore { (store, root) =>
      val payload = payloadOf("real-blob")
      val digest = RecoveryBlobStore.sha256(payload)
      store.put(appId, digest, payload)
      age(root, appId, digest, graceMs * 2)

      val stray = Paths.get(
        root.getAbsolutePath,
        RecoveryBlobStore.BlobDirectory,
        appId,
        "zz")
      Files.createDirectories(stray)
      Files.write(stray.resolve(s"not-a-digest${RecoveryBlobStore.BlobSuffix}"), payload)

      val collector = new RecoveryBlobCollector(store, (_, _) => false, graceMs)
      assert(collector.collect() == 1, "only the real blob is collected")
      assert(Files.exists(stray.resolve(s"not-a-digest${RecoveryBlobStore.BlobSuffix}")))
    }
  }

  test("hex parsing rejects anything that is not a full digest") {
    assert(RecoveryBlobCollector.parseHex(null) === null)
    assert(RecoveryBlobCollector.parseHex("abcd") === null)
    assert(RecoveryBlobCollector.parseHex("g" * 64) === null)
    assert(RecoveryBlobCollector.parseHex("0" * 64) !== null)
  }
}
