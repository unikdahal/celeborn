/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.celeborn.service.deploy.master.clustermeta

import java.nio.file.Files
import java.security.MessageDigest
import java.util.Collections

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf

/**
 * The inline budget has three nested bounds - per recovery, per application, cluster-wide - and
 * each rejection must say which bound fired: an operator who cannot tell a selfish application
 * from a full cluster tunes the wrong knob. The per-application share (T-14) stops one wide or
 * repeatedly retried write from starving every other resumable write.
 *
 * A minimal pointer serializes to roughly 78 bytes, so the byte quotas below are chosen with wide
 * margins around multiples of that size.
 */
class RecoveryInlineCapacitySuite extends AnyFunSuite {

  private def metadata(conf: CelebornConf) = new SingleMasterMetaManager(null, conf)

  private def digestOf(text: String): Array[Byte] =
    MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))

  private def confWith(bytesPerApp: String, extra: (String, String)*): CelebornConf = {
    val conf = new CelebornConf()
      .set(CelebornConf.RECOVERY_TASK_COMMIT_MAX_INLINE_BYTES_PER_APP.key, bytesPerApp)
    extra.foreach { case (key, value) => conf.set(key, value) }
    conf
  }

  private def publishPointer(
      meta: SingleMasterMetaManager,
      appId: String,
      recoveryId: String,
      payloadTag: String): Unit = {
    meta.updateRecoveryBlobPointerMeta(
      appId,
      recoveryId,
      "write-1",
      0,
      digestOf(payloadTag),
      4096L,
      1,
      Collections.singletonList("worker-1"),
      100L)
  }

  test("one application cannot consume more than its share of the inline budget") {
    val meta = metadata(confWith("128b"))

    publishPointer(meta, "app-a", "query-1", "first")
    val rejected = intercept[IllegalStateException] {
      publishPointer(meta, "app-a", "query-2", "second")
    }
    assert(
      rejected.getMessage.contains("exceeded its recovery inline metadata quota"),
      s"an application-quota rejection must name itself: ${rejected.getMessage}")
    assert(
      rejected.getMessage.contains("maxInlineBytesPerApp"),
      "the message must name the setting that governs the share")
    // A different application is untouched by app-a's exhaustion: that is the point of the share.
    publishPointer(meta, "app-b", "query-1", "other-app")
  }

  test("cluster-wide rejections are distinguishable from application-quota rejections") {
    val meta = metadata(
      confWith(
        "256m",
        CelebornConf.RECOVERY_TASK_COMMIT_MAX_INLINE_BYTES_GLOBAL.key -> "120b"))
    // Each application stays far inside its own share, but together they overflow the cluster:
    // the second publication must report the cluster-wide bound.
    publishPointer(meta, "app-a", "query-1", "a-payload")
    val rejected = intercept[IllegalStateException] {
      publishPointer(meta, "app-b", "query-1", "b-payload")
    }
    assert(
      rejected.getMessage.contains("Cluster-wide"),
      s"a global rejection must say so: ${rejected.getMessage}")
  }

  test("released capacity restores the application's share") {
    val meta = metadata(confWith("100b"))
    publishPointer(meta, "app-a", "query-1", "payload")

    meta.updateAppLostMeta("app-a")

    // Without the release, a second recovery of the same application would still be blocked even
    // though nothing of the first one survives.
    publishPointer(meta, "app-a", "query-2", "fresh-start")
  }

  test("per-application usage survives an HA snapshot restore") {
    val conf = confWith("165b")
    val leader = metadata(conf)
    publishPointer(leader, "app-a", "query-1", "first-generation")
    publishPointer(leader, "app-a", "query-2", "second-generation")

    val snapshot = Files.createTempFile("celeborn-app-quota-snapshot", ".bin")
    try {
      leader.writeMetaInfoToFile(snapshot.toFile)
      val follower = metadata(conf)
      follower.restoreMetaFromFile(snapshot.toFile)

      // A follower that rebuilt its shares would let the application through here; the restored
      // counter must refuse it exactly as the leader would.
      val rejected = intercept[IllegalStateException] {
        publishPointer(follower, "app-a", "query-3", "after-failover")
      }
      assert(rejected.getMessage.contains("exceeded its recovery inline metadata quota"))
    } finally {
      Files.deleteIfExists(snapshot)
    }
  }
}
