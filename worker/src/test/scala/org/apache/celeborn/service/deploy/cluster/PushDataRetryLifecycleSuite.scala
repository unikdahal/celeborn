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
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import io.netty.buffer.{ByteBuf, Unpooled}
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
 * Pushes batches to a mini cluster whose small hard split threshold makes workers reject
 * in-flight batches, so the client revives partitions and resends those batches. Every batch
 * body must be released exactly once afterwards, and every batch must be read back once.
 */
class PushDataRetryLifecycleSuite extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {

  private var masterPort = 0

  override def beforeAll(): Unit = {
    val workerConf = Map(CelebornConf.WORKER_PARTITION_SPLIT_MIN_SIZE.key -> "64k")
    val (m, _) = setupMiniClusterWithRandomPorts(workerConf = workerConf)
    masterPort = m.conf.masterPort
  }

  override def afterAll(): Unit = {
    shutdownMiniCluster()
  }

  test("batches resent after hard splits are released once and read back once") {
    val app = s"push-retry-${System.nanoTime()}"
    val conf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, CompressionCodec.NONE.name)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
      .set(CelebornConf.SHUFFLE_PARTITION_SPLIT_MODE.key, "HARD")
      .set(CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key, "64k")
      .set("celeborn.data.io.numConnectionsPerPeer", "1")
    val bodies = new ConcurrentLinkedQueue[ByteBuf]()
    val lifecycleManager = new LifecycleManager(app, conf)
    val client = new ShuffleClientImpl(app, conf, UserIdentifier("mock", "mock")) {
      override protected def newBatchBody(framedBatch: Array[Byte]): ByteBuf = {
        val body = Unpooled.directBuffer(framedBatch.length).writeBytes(framedBatch)
        bodies.add(body)
        body
      }
    }
    client.setupLifecycleManagerRef(lifecycleManager.self)
    try {
      val shuffleId = 1
      val numMappers = 2
      val numPartitions = 4
      val random = new Random(11)
      val expected = Array.fill(numPartitions)(mutable.Map[(Int, Int), Seq[Byte]]())
      for (mapId <- 0 until numMappers) {
        for (batch <- 0 until 64) {
          val partition = random.nextInt(numPartitions)
          // Each batch starts with its length and identity so that the read stream, in which
          // batches of different mappers and split locations interleave, can be parsed back.
          val bytes = new Array[Byte](12 + random.nextInt(32 * 1024))
          random.nextBytes(bytes)
          ByteBuffer.wrap(bytes).putInt(bytes.length).putInt(mapId).putInt(batch)
          expected(partition)((mapId, batch)) = bytes.toSeq
          client.pushData(
            shuffleId,
            mapId,
            0,
            partition,
            bytes,
            0,
            bytes.length,
            numMappers,
            numPartitions)
        }
        client.mapperEnd(shuffleId, mapId, 0, numMappers, numPartitions)
      }

      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
      while (bodies.asScala.exists(_.refCnt() != 0) && System.nanoTime() < deadline) {
        Thread.sleep(10)
      }
      val leaked = bodies.asScala.count(_.refCnt() != 0)
      Assert.assertEquals(s"$leaked of ${bodies.size} batch bodies were never released", 0, leaked)
      Assert.assertEquals(numMappers * 64, bodies.size)

      val locations = lifecycleManager.workerSnapshots(shuffleId).values().asScala
        .map(_.getPrimaryPartitions().size()).sum
      Assert.assertTrue(
        s"expected hard splits to revive partitions, but found $locations locations",
        locations > numPartitions)

      val metricsCallback = new MetricsCallback {
        override def incBytesRead(bytesWritten: Long): Unit = {}
        override def incReadTime(time: Long): Unit = {}
      }
      for (partition <- 0 until numPartitions) {
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
        val read = ByteBuffer.wrap(out.toByteArray)
        val actual = mutable.Map[(Int, Int), Seq[Byte]]()
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
    } finally {
      client.shutdown()
      lifecycleManager.stop()
    }
  }
}
