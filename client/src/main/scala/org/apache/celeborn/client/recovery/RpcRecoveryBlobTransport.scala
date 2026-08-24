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

package org.apache.celeborn.client.recovery

import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.RpcNameConstants
import org.apache.celeborn.common.protocol.message.ControlMessages.{FetchRecoveryBlob, FetchRecoveryBlobResponse, PushRecoveryBlob, PushRecoveryBlobResponse}
import org.apache.celeborn.common.rpc.{RpcAddress, RpcEndpointAddress, RpcEndpointRef, RpcEnv}

/**
 * Carries recovery blobs to and from workers over Celeborn's control RPC.
 *
 * Worker identity is the `host:rpcPort` string a pointer records, so a pointer read on a
 * replacement driver resolves without any other lookup. Endpoint references are cached because a
 * wide write touches the same handful of workers thousands of times.
 */
private[celeborn] class RpcRecoveryBlobTransport(
    applicationId: String,
    rpcEnv: RpcEnv,
    conf: CelebornConf) extends RecoveryBlobTransport with Logging {

  private val endpoints = new ConcurrentHashMap[String, RpcEndpointRef]()

  override def upload(workerId: String, digest: Array[Byte], payload: Array[Byte]): Unit = {
    val response = endpointFor(workerId).askSync[PushRecoveryBlobResponse](
      PushRecoveryBlob(applicationId, digest, payload))
    if (!response.success) {
      throw new IOException(
        s"Worker $workerId refused a recovery blob: ${response.reason}")
    }
  }

  override def fetch(workerId: String, digest: Array[Byte]): Array[Byte] = {
    val response = endpointFor(workerId).askSync[FetchRecoveryBlobResponse](
      FetchRecoveryBlob(applicationId, digest))
    if (!response.success) {
      // The worker holds the blob but could not serve it. That is a corruption or IO signal, not
      // an absence, and the caller must be able to tell them apart.
      throw new IOException(s"Worker $workerId could not serve a recovery blob: ${response.reason}")
    }

    if (response.found) response.payload else null
  }

  private def endpointFor(workerId: String): RpcEndpointRef = {
    endpoints.computeIfAbsent(
      workerId,
      (id: String) => {
        val separator = id.lastIndexOf(':')
        require(separator > 0, s"Malformed recovery blob worker id: $id")
        val host = id.substring(0, separator)
        val port = id.substring(separator + 1).toInt
        rpcEnv.setupEndpointRefByAddr(
          RpcEndpointAddress(RpcAddress(host, port), RpcNameConstants.WORKER_EP))
      })
  }
}

private[celeborn] object RpcRecoveryBlobTransport {

  /** The identity a pointer records for a worker, and the only thing needed to reach it again. */
  def workerId(worker: WorkerInfo): String = s"${worker.host}:${worker.rpcPort}"
}
