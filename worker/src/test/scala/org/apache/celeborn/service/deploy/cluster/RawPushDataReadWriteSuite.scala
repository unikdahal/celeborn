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

package org.apache.celeborn.service.deploy.cluster

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.util.Random

import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.CompressionCodec
import org.apache.celeborn.service.deploy.MiniClusterFeature

/**
 * Pushes caller-owned direct buffers to a mini cluster and reads them back. Hard splits force the
 * client to revive partitions and resend in-flight batches, so every payload must outlive its
 * retries and be released exactly once afterwards.
 */
class RawPushDataReadWriteSuite extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {

  private var masterPort = 0

  override def beforeAll(): Unit = {
    val workerConf = Map(
      CelebornConf.WORKER_PARTITION_SPLIT_MIN_SIZE.key -> "64k")
    val (m, _) = setupMiniClusterWithRandomPorts(workerConf = workerConf)
    masterPort = m.conf.masterPort
  }

  override def afterAll(): Unit = {
    shutdownMiniCluster()
  }

  private def clientConf(app: String): CelebornConf = new CelebornConf()
    .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
    .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, CompressionCodec.NONE.name)
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
    .set(CelebornConf.SHUFFLE_PARTITION_SPLIT_MODE.key, "HARD")
    .set(CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key, "64k")
    .set(CelebornConf.CLIENT_SHUFFLE_INTEGRITY_CHECK_ENABLED.key, "true")
    .set("celeborn.data.io.numConnectionsPerPeer", "1")

  private def readPartition(
      client: ShuffleClientImpl,
      shuffleId: Int,
      partition: Int): Array[Byte] = {
    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesWritten: Long): Unit = {}
      override def incReadTime(time: Long): Unit = {}
    }
    val in = client.readPartition(
      shuffleId,
      shuffleId,
      partition,
      0,
      0,
      0,
      Integer.MAX_VALUE,
      null,
      null,
      null,
      null,
      null,
      null,
      metricsCallback,
      true)
    val out = new ByteArrayOutputStream()
    val chunk = new Array[Byte](64 * 1024)
    var n = in.read(chunk)
    while (n != -1) {
      out.write(chunk, 0, n)
      n = in.read(chunk)
    }
    in.close()
    out.toByteArray
  }

  test("caller-owned direct buffers survive hard-split retries and are released once") {
    val app = s"raw-push-${System.nanoTime()}"
    val conf = clientConf(app)
    val lifecycleManager = new LifecycleManager(app, conf)
    val client = new ShuffleClientImpl(app, conf, UserIdentifier("mock", "mock"))
    client.setupLifecycleManagerRef(lifecycleManager.self)
    try {
      val shuffleId = 1
      val numMappers = 2
      val numPartitions = 4
      val random = new Random(7)
      val expected = Array.fill(numPartitions)(
        scala.collection.mutable.Map[(Int, Int), Seq[Byte]]())
      val pending = new AtomicInteger()
      val doubleReleases = new AtomicInteger()
      val allReleased = new CountDownLatch(1)
      val outstanding = new ConcurrentLinkedQueue[ByteBuffer]()

      for (mapId <- 0 until numMappers) {
        for (batch <- 0 until 64) {
          val partition = random.nextInt(numPartitions)
          // Each payload starts with its length and identity so that the read stream, in which
          // batches of different mappers and split locations interleave, can be parsed back.
          val bytes = new Array[Byte](12 + random.nextInt(32 * 1024))
          random.nextBytes(bytes)
          ByteBuffer.wrap(bytes).putInt(bytes.length).putInt(mapId).putInt(batch)
          expected(partition)((mapId, batch)) = bytes.toSeq
          val direct = ByteBuffer.allocateDirect(bytes.length)
          direct.put(bytes).flip()
          outstanding.add(direct)
          pending.incrementAndGet()
          val released = new AtomicInteger()
          val pushed = client.pushRawData(
            shuffleId,
            mapId,
            0,
            partition,
            direct,
            numMappers,
            numPartitions,
            new Runnable {
              override def run(): Unit = {
                if (released.incrementAndGet() != 1) {
                  doubleReleases.incrementAndGet()
                }
                // Poison the payload once released: a later retry would send corrupt data.
                while (direct.hasRemaining) direct.put(0.toByte)
                outstanding.remove(direct)
                if (pending.decrementAndGet() == 0) allReleased.countDown()
              }
            })
          Assert.assertEquals(bytes.length + 16, pushed)
        }
        client.mapperEnd(shuffleId, mapId, 0, numMappers, numPartitions)
      }

      Assert.assertTrue(
        s"${pending.get()} payloads were never released",
        allReleased.await(60, TimeUnit.SECONDS))
      Assert.assertEquals(0, doubleReleases.get())
      Assert.assertTrue(outstanding.isEmpty)

      // The writer recorded CRCs; reading needs no end-of-partition integrity handshake here.
      val readConf =
        conf.clone.set(CelebornConf.CLIENT_SHUFFLE_INTEGRITY_CHECK_ENABLED.key, "false")
      val reader = new ShuffleClientImpl(app, readConf, UserIdentifier("mock", "mock"))
      reader.setupLifecycleManagerRef(lifecycleManager.self)
      for (partition <- 0 until numPartitions) {
        val read = ByteBuffer.wrap(readPartition(reader, shuffleId, partition))
        val actual = scala.collection.mutable.Map[(Int, Int), Seq[Byte]]()
        while (read.hasRemaining) {
          val start = read.position()
          val length = read.getInt()
          val key = (read.getInt(), read.getInt())
          val payload = new Array[Byte](length)
          read.position(start)
          read.get(payload)
          Assert.assertTrue(s"duplicate batch $key", actual.put(key, payload.toSeq).isEmpty)
        }
        Assert.assertEquals(s"partition $partition", expected(partition), actual)
      }
      val splits = lifecycleManager.workerSnapshots(shuffleId).values().asScala
        .map(_.getPrimaryPartitions().size()).sum
      logInfo(s"partition locations after hard splits: $splits")
      reader.shutdown()
    } finally {
      client.shutdown()
      lifecycleManager.stop()
    }
  }
}
