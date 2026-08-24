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

package org.apache.celeborn.service.deploy.master

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.RpcNameConstants
import org.apache.celeborn.common.protocol.message.ControlMessages.{ReplicateRecoveryBlob, ReplicateRecoveryBlobResponse}
import org.apache.celeborn.common.rpc.{RpcAddress, RpcEndpointAddress, RpcEndpointRef, RpcEnv}

/**
 * Asks the worker that still holds a payload to copy it to the workers that should.
 *
 * The master decides which payloads are under-replicated but never carries their bytes: moving them
 * through master memory would reintroduce exactly the pressure the blob backend exists to remove.
 */
private[master] class RpcRecoveryBlobRepairExecutor(rpcEnv: RpcEnv)
  extends RecoveryBlobRepairExecutor with Logging {

  private val peers = new ConcurrentHashMap[String, RpcEndpointRef]()

  override def replicate(task: RecoveryBlobRepairTask): Seq[String] = {
    val response = peer(task.source).askSync[ReplicateRecoveryBlobResponse](
      ReplicateRecoveryBlob(
        task.pointer.getAppId,
        task.pointer.getSha256.toByteArray,
        task.targets.asJava))

    if (!response.success) {
      // The source could not serve the payload, so it is no longer a usable replica. Reporting
      // nothing accepted leaves the pointer untouched and lets the next cycle pick another source.
      logWarning(
        s"Worker ${task.source} could not replicate a recovery blob: ${response.reason}")
      Seq.empty
    } else {
      response.acceptedWorkerIds.asScala.toSeq
    }
  }

  private def peer(workerId: String): RpcEndpointRef = {
    peers.computeIfAbsent(
      workerId,
      (id: String) => {
        val separator = id.lastIndexOf(':')
        require(separator > 0, s"Malformed recovery blob worker id: $id")
        rpcEnv.setupEndpointRefByAddr(
          RpcEndpointAddress(
            RpcAddress(id.substring(0, separator), id.substring(separator + 1).toInt),
            RpcNameConstants.WORKER_EP))
      })
  }
}
