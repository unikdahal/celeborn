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

package org.apache.celeborn.client

import scala.util.control.NonFatal

import org.apache.celeborn.common.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

/** Internal JVM-only PoC messages, not a versioned public Celeborn wire protocol. */
private[celeborn] object RetainedShuffleControl {
  val EndpointName = "RetainedShuffleControlV1"

  sealed trait Request extends Serializable { def incarnation: String }
  case class Acquire(incarnation: String, shuffleId: Int, ttlMillis: Long) extends Request
  case class Renew(incarnation: String, lease: RetainedShuffleLease, ttlMillis: Long) extends Request
  case class Release(incarnation: String, lease: RetainedShuffleLease) extends Request
  case class Probe(incarnation: String) extends Request
  case class Response(
      incarnation: String,
      accepted: Boolean,
      lease: Option[RetainedShuffleLease],
      reason: String) extends Serializable
}

private[celeborn] final class RetainedShuffleControlEndpoint(
    service: RetainedShuffleService) extends RpcEndpoint {
  import RetainedShuffleControl._

  override val rpcEnv: RpcEnv = service.lifecycleManager.rpcEnv

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case request: Request =>
      try {
        val response = if (request.incarnation != service.incarnation || !service.isLive) {
          Response(service.incarnation, false, None, "service incarnation is not live")
        } else request match {
          case Acquire(_, shuffleId, ttlMillis) =>
            val lease = service.retain(shuffleId, ttlMillis)
            Response(service.incarnation, lease.nonEmpty, lease,
              if (lease.nonEmpty) "retained" else "shuffle absent or retention capacity exhausted")
          case Renew(_, lease, ttlMillis) =>
            val renewed = service.renew(lease, ttlMillis)
            Response(service.incarnation, renewed, if (renewed) Some(lease) else None,
              if (renewed) "renewed" else "lease expired or unknown")
          case Release(_, lease) =>
            service.release(lease)
            Response(service.incarnation, true, None, "released")
          case Probe(_) => Response(service.incarnation, true, None, "live")
        }
        context.reply(response)
      } catch {
        case NonFatal(error) => context.sendFailure(error)
      }
  }
}

/** Reuses an existing Celeborn RPC environment; this client never owns or stops the service. */
private[celeborn] final class RetainedShuffleControlClient(
    rpcEnv: RpcEnv,
    host: String,
    port: Int,
    incarnation: String) {
  import RetainedShuffleControl._
  require(rpcEnv != null && host != null && host.nonEmpty)
  require(port > 0 && port <= 65535 && incarnation != null && incarnation.nonEmpty)

  private val endpoint: RpcEndpointRef =
    rpcEnv.setupEndpointRef(RpcAddress(host, port), EndpointName)

  private def request(message: Request): Response = {
    val response = endpoint.askSync[Response](message)
    require(response != null && response.incarnation == incarnation,
      "retention response came from another service incarnation")
    response
  }

  def probe(): Boolean = request(Probe(incarnation)).accepted
  def acquire(shuffleId: Int, ttlMillis: Long): Option[RetainedShuffleLease] = {
    val response = request(Acquire(incarnation, shuffleId, ttlMillis))
    if (response.accepted) {
      require(response.lease.exists(_.shuffleId == shuffleId), "retention response changed shuffle")
      response.lease
    } else None
  }
  def renew(lease: RetainedShuffleLease, ttlMillis: Long): Boolean = {
    val response = request(Renew(incarnation, lease, ttlMillis))
    if (response.accepted) require(response.lease.contains(lease), "renewal changed the lease")
    response.accepted
  }
  def release(lease: RetainedShuffleLease): Unit = {
    require(request(Release(incarnation, lease)).accepted, "retention release was rejected")
  }
}
