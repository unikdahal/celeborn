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

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol.CompressionCodec
import org.apache.celeborn.service.deploy.MiniClusterFeature

/** Times pushData(byte[]) against a one-worker mini cluster; prints one RESULT line per run. */
object PushDataPathBenchmark extends MiniClusterFeature {
  def main(args: Array[String]): Unit = {
    val label = args(0)
    val threads =
      ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    val os = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    def allocated(): Long = threads.getThreadAllocatedBytes(threads.getAllThreadIds).filter(_ > 0).sum
    val (master, _) = setupMiniClusterWithRandomPorts(workerNum = 1)
    val app = s"push-path-${System.nanoTime()}"
    val conf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:${master.conf.masterPort}")
      .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, CompressionCodec.NONE.name)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
    val lifecycleManager = new LifecycleManager(app, conf)
    val client = new ShuffleClientImpl(app, conf, UserIdentifier("mock", "mock"))
    client.setupLifecycleManagerRef(lifecycleManager.self)
    var shuffleId = 0
    def run(batchSize: Int): (Double, Double, Double) = {
      shuffleId += 1
      val batches = (256L * 1024 * 1024 / batchSize).toInt
      val batch = new Array[Byte](batchSize)
      System.gc()
      val a0 = allocated()
      val c0 = os.getProcessCpuTime
      val t0 = System.nanoTime()
      var i = 0
      while (i < batches) {
        client.pushData(shuffleId, 0, 0, i % 16, batch, 0, batchSize, 1, 16)
        i += 1
      }
      client.mapperEnd(shuffleId, 0, 0, 1, 16)
      val result = (
        (System.nanoTime() - t0) / 1e6,
        (os.getProcessCpuTime - c0) / 1e6,
        (allocated() - a0) / 1048576.0)
      lifecycleManager.unregisterShuffle(shuffleId)
      result
    }
    for (size <- Seq(64 * 1024, 1024 * 1024)) {
      run(size)
      run(size)
      for (_ <- 0 until 3) {
        val (wall, cpu, alloc) = run(size)
        // scalastyle:off println
        println(f"RESULT $label ${size / 1024} $wall%.1f $cpu%.1f $alloc%.1f")
        // scalastyle:on println
      }
    }
    System.exit(0)
  }
}
