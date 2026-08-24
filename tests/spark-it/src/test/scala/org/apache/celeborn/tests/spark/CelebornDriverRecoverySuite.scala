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

package org.apache.celeborn.tests.spark

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.protocol.ShuffleMode

/**
 * Driver-recovery integration tests: a second driver, sharing only Celeborn's durable state with
 * the first, must adopt the shuffle stages the first driver committed instead of recomputing them.
 *
 * The measurement is task counts. A stage that completes having run zero tasks was adopted; wall
 * time proves nothing. Each test therefore installs a listener that records the number of tasks per
 * completed stage in the replacement driver.
 *
 * The lease here is deliberately short. A replacement driver cannot take ownership while the
 * previous lease is still valid and held by another owner, and the first driver renews every
 * third of the lease duration, so a production-sized lease would make every test wait minutes.
 */
class CelebornDriverRecoverySuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  private val leaseDuration = "5s"
  private val leaseExpiryWaitMs = 7000L

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    stopActiveSparkSessions()
    System.gc()
  }

  private def recoveryConf(recoveryId: String, applicationId: String): SparkConf = {
    val sparkConf = new SparkConf()
      .setAppName("celeborn-driver-recovery")
      .setMaster("local[2]")
    updateSparkConf(sparkConf, ShuffleMode.HASH)
    sparkConf.set("spark.sql.extensions",
      "org.apache.spark.shuffle.celeborn.CelebornShuffleStageRecoveryExtension")
    sparkConf.set("spark.celeborn.driverRecovery.enabled", "true")
    sparkConf.set("spark.celeborn.driverRecovery.id", recoveryId)
    sparkConf.set("spark.celeborn.driverRecovery.leaseDuration", leaseDuration)
    sparkConf.set("spark.celeborn.driverRecovery.probeTimeout", "5s")
    sparkConf.set("spark.celeborn.client.application.uniqueId", applicationId)
    sparkConf
  }

  /** Runs the workload and returns its result plus the task count of every completed stage. */
  private def runWorkload(sparkConf: SparkConf): (collection.Map[Any, Int], Seq[Int]) = {
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    val tasksByStage = new ConcurrentHashMap[Int, AtomicInteger]()
    val completedStageTasks = Seq.newBuilder[Int]
    session.sparkContext.addSparkListener(new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
        tasksByStage.computeIfAbsent(taskEnd.stageId, _ => new AtomicInteger(0)).incrementAndGet()

      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
        completedStageTasks +=
          Option(tasksByStage.get(stageCompleted.stageInfo.stageId)).map(_.get()).getOrElse(0)
    })
    try {
      val result = repartition(session)
      // Stage completion callbacks are asynchronous; give the listener bus time to drain before
      // the counts are read.
      session.sparkContext.listenerBus.waitUntilEmpty()
      (result, completedStageTasks.result())
    } finally {
      session.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      ShuffleClient.reset()
    }
  }

  test("a replacement driver adopts a committed shuffle stage") {
    val recoveryId = s"recovery-${UUID.randomUUID()}"
    val applicationId = s"app-${UUID.randomUUID()}"

    val (firstResult, firstStageTasks) = runWorkload(recoveryConf(recoveryId, applicationId))
    assert(firstStageTasks.forall(_ > 0),
      "the first driver has nothing to adopt, so every stage must run tasks")

    // The first driver's lease outlives its process. Ownership can only move once it expires.
    Thread.sleep(leaseExpiryWaitMs)

    val (secondResult, secondStageTasks) = runWorkload(recoveryConf(recoveryId, applicationId))
    assert(secondResult === firstResult,
      "a recovered execution must produce exactly the result of an uninterrupted one")
    assert(secondStageTasks.exists(_ == 0),
      s"expected at least one adopted stage with zero tasks, got $secondStageTasks")
  }

  test("a different recovery identity adopts nothing") {
    val applicationId = s"app-${UUID.randomUUID()}"

    val (firstResult, _) = runWorkload(recoveryConf(s"recovery-${UUID.randomUUID()}", applicationId))
    Thread.sleep(leaseExpiryWaitMs)

    val (secondResult, secondStageTasks) =
      runWorkload(recoveryConf(s"recovery-${UUID.randomUUID()}", applicationId))
    assert(secondResult === firstResult)
    assert(secondStageTasks.forall(_ > 0),
      s"a different recovery identity must not adopt anything, got $secondStageTasks")
  }
}
