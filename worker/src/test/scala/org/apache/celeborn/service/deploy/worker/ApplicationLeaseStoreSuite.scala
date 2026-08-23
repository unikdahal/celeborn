/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.celeborn.service.deploy.worker

import java.io.{File, FileOutputStream, IOException}
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.meta.ApplicationLease

class ApplicationLeaseStoreSuite extends AnyFunSuite {

  private def withStore(testCode: (File, ApplicationLeaseStore) => Unit): Unit = {
    val root = Files.createTempDirectory("celeborn-worker-leases").toFile
    val store = new ApplicationLeaseStore(root)
    try testCode(root, store)
    finally {
      store.close()
      FileUtils.deleteDirectory(root)
    }
  }

  test("persist fences across worker restart and reject stale owners") {
    withStore { (root, store) =>
      val lease = new ApplicationLease(3L, "driver-b", 5000L)
      assert(store.install("app-1", lease) == lease)
      store.validate("app-1", 3L, "driver-b", 4999L)
      intercept[IllegalStateException](store.validate("app-1", 2L, "driver-a", 1000L))
      intercept[IllegalStateException](store.validate("app-1", 3L, "driver-a", 1000L))
      intercept[IllegalStateException](store.validate("app-1", 3L, "driver-b", 5000L))

      val restarted = new ApplicationLeaseStore(root)
      try {
        assert(restarted.current("app-1") == lease)
        intercept[IllegalStateException](
          restarted.install("app-1", new ApplicationLease(2L, "driver-a", 6000L)))
      } finally restarted.close()
    }
  }

  test("allow exact replay and monotonic renewal but reject same-epoch owner changes") {
    withStore { (_, store) =>
      val initial = new ApplicationLease(1L, "driver-a", 100L)
      assert(store.install("app-1", initial) == initial)
      assert(store.install("app-1", initial) == initial)
      val renewed = new ApplicationLease(1L, "driver-a", 200L)
      assert(store.install("app-1", renewed) == renewed)
      intercept[IllegalArgumentException](
        store.install("app-1", new ApplicationLease(1L, "driver-a", 199L)))
      intercept[IllegalStateException](
        store.install("app-1", new ApplicationLease(1L, "driver-b", 300L)))
    }
  }

  test("allow legacy mutations only before an application is fenced") {
    withStore { (_, store) =>
      store.validate("legacy-app", 0L, "", 1L)
      intercept[IllegalStateException](store.validate("legacy-app", 1L, "driver", 1L))
      store.install("legacy-app", new ApplicationLease(1L, "driver", 100L))
      intercept[IllegalStateException](store.validate("legacy-app", 0L, "", 1L))
    }
  }

  test("fail closed when durable lease state is corrupted") {
    val root = Files.createTempDirectory("celeborn-worker-leases-corrupt").toFile
    try {
      val output = new FileOutputStream(new File(root, "application-leases.bin"))
      try output.write(Array[Byte](1, 2, 3))
      finally output.close()
      intercept[IOException](new ApplicationLeaseStore(root))
    } finally FileUtils.deleteDirectory(root)
  }

  test("reject fence installation without a stable worker identity") {
    val root = Files.createTempDirectory("celeborn-worker-leases-unstable").toFile
    try {
      val store = new ApplicationLeaseStore(root, durableIdentity = false)
      intercept[IllegalStateException](
        store.install("app-1", new ApplicationLease(1L, "driver-a", 100L)))
    } finally FileUtils.deleteDirectory(root)
  }
}
