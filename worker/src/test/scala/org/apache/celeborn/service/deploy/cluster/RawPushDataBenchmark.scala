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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.apache.celeborn.benchmark.{Benchmark, BenchmarkBase}
import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol.CompressionCodec
import org.apache.celeborn.service.deploy.MiniClusterFeature

/**
 * Compares the cost of pushing already-compressed batches through `pushData(byte[])`, which
 * frames each batch into a new array, and `pushRawData`, which frames a caller-owned heap or
 * direct buffer without copying it.
 *
 * Each round runs every mode once, in rotating order, and each shuffle is unregistered afterwards.
 * That keeps disk writeback and accumulated data from favoring whichever mode runs first. The report gives the median and
 * interquartile range over the rounds of wall time, process CPU time and the bytes allocated by
 * all JVM threads. The worker runs in the same JVM; its share of CPU and allocation is the same
 * for every mode, so differences between modes come from the client.
 *
 * {{{
 *   To run this benchmark:
 *   1. build/mvn -pl worker -am install -DskipTests
 *   2. CELEBORN_GENERATE_BENCHMARK_FILES=1 build/mvn -pl worker \
 *        org.codehaus.mojo:exec-maven-plugin:3.1.0:java -Dexec.classpathScope=test \
 *        -Dexec.mainClass=org.apache.celeborn.service.deploy.cluster.RawPushDataBenchmark
 *      Results will be written to "benchmarks/RawPushDataBenchmark-results.txt".
 * }}}
 */
object RawPushDataBenchmark extends BenchmarkBase with MiniClusterFeature {

  private val threads =
    ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
  private val os =
    ManagementFactory.getOperatingSystemMXBean.asInstanceOf[
      com.sun.management.OperatingSystemMXBean]

  private def allocatedBytes(): Long =
    threads.getThreadAllocatedBytes(threads.getAllThreadIds).filter(_ > 0).sum

  private val modes = Seq("pushData(byte[])", "pushRawData(heap)", "pushRawData(direct)")
  private val rounds = 7
  private var shuffleIds = 0

  private case class Sample(millis: Double, cpuMillis: Double, allocatedMiB: Double)

  private def pushAll(
      lifecycleManager: LifecycleManager,
      client: ShuffleClientImpl,
      mode: String,
      batch: Array[Byte],
      direct: ByteBuffer,
      batches: Int,
      numPartitions: Int): Sample = {
    shuffleIds += 1
    val shuffleId = shuffleIds
    val pending = new AtomicInteger()
    val release = new Runnable { override def run(): Unit = pending.decrementAndGet() }
    System.gc()
    val allocatedBefore = allocatedBytes()
    val cpuBefore = os.getProcessCpuTime
    val start = System.nanoTime()
    var i = 0
    while (i < batches) {
      val partition = i % numPartitions
      mode match {
        case "pushData(byte[])" =>
          client.pushData(shuffleId, 0, 0, partition, batch, 0, batch.length, 1, numPartitions)
        case "pushRawData(heap)" =>
          pending.incrementAndGet()
          client.pushRawData(
            shuffleId,
            0,
            0,
            partition,
            ByteBuffer.wrap(batch),
            1,
            numPartitions,
            release)
        case _ =>
          pending.incrementAndGet()
          client.pushRawData(
            shuffleId,
            0,
            0,
            partition,
            direct.duplicate(),
            1,
            numPartitions,
            release)
      }
      i += 1
    }
    client.mapperEnd(shuffleId, 0, 0, 1, numPartitions)
    val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2)
    while (pending.get() != 0) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException(s"${pending.get()} pushed buffers were never released")
      }
      Thread.sleep(1)
    }
    val sample = Sample(
      (System.nanoTime() - start) / 1e6,
      (os.getProcessCpuTime - cpuBefore) / 1e6,
      (allocatedBytes() - allocatedBefore) / 1048576.0)
    lifecycleManager.unregisterShuffle(shuffleId)
    sample
  }

  private def summary(values: scala.collection.Seq[Double]): String = {
    val sorted = values.sorted
    def at(q: Double): Double = {
      val position = q * (sorted.size - 1)
      val low = sorted(position.toInt)
      val high = sorted(math.min(position.toInt + 1, sorted.size - 1))
      low + (high - low) * (position - position.toInt)
    }
    f"${at(0.5)}%9.1f (${at(0.25)}%.1f-${at(0.75)}%.1f)"
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val (master, _) = setupMiniClusterWithRandomPorts(workerNum = 1)
    val app = s"raw-push-benchmark-${System.nanoTime()}"
    val conf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:${master.conf.masterPort}")
      .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, CompressionCodec.NONE.name)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
    val lifecycleManager = new LifecycleManager(app, conf)
    val client = new ShuffleClientImpl(app, conf, UserIdentifier("mock", "mock"))
    client.setupLifecycleManagerRef(lifecycleManager.self)
    try {
      val numPartitions = 16
      for (batchSize <- Seq(64 * 1024, 1024 * 1024, 4 * 1024 * 1024)) {
        val batches = (256L * 1024 * 1024 / batchSize).toInt
        val batch = new Array[Byte](batchSize)
        new java.util.Random(1).nextBytes(batch)
        val direct = ByteBuffer.allocateDirect(batchSize)
        direct.put(batch).flip()
        val label = s"Push $batches x ${batchSize / 1024} KiB batches (256 MiB per run)"
        runBenchmark(label) {
          val samples = mutable.Map[String, mutable.ArrayBuffer[Sample]]()
          // One warm-up round, then rounds that rotate which mode goes first.
          modes.foreach(mode =>
            pushAll(lifecycleManager, client, mode, batch, direct, batches, numPartitions))
          for (round <- 0 until rounds) {
            val order = modes.drop(round % modes.size) ++ modes.take(round % modes.size)
            order.foreach { mode =>
              samples.getOrElseUpdate(mode, mutable.ArrayBuffer()) +=
                pushAll(lifecycleManager, client, mode, batch, direct, batches, numPartitions)
            }
          }
          val report = new StringBuilder()
          report.append(Benchmark.getJVMOSInfo()).append('\n')
          report.append(Benchmark.getProcessorName()).append('\n')
          report.append(s"$rounds rounds, mode order rotated every round\n")
          report.append(
            f"${"mode"}%-22s ${"wall ms, median (IQR)"}%26s ${"process CPU ms"}%26s " +
              f"${"allocated MiB"}%26s\n")
          modes.foreach { mode =>
            val s = samples(mode)
            report.append(
              f"$mode%-22s ${summary(s.map(_.millis))}%26s ${summary(s.map(_.cpuMillis))}%26s " +
                f"${summary(s.map(_.allocatedMiB))}%26s\n")
          }
          // scalastyle:off println
          println(report)
          // scalastyle:on println
          output.foreach(_.write(report.toString.getBytes))
        }
      }
    } finally {
      client.shutdown()
      lifecycleManager.stop()
      shutdownMiniCluster()
    }
  }

  override def afterAll(): Unit = {
    // The mini cluster leaves non-daemon threads behind; exit once the results are written.
    System.exit(0)
  }
}
