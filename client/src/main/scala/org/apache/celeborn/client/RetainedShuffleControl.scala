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

import java.util.UUID

import scala.util.control.NonFatal

import org.apache.celeborn.common.identity.UserIdentifier

import org.apache.celeborn.common.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

/** Internal JVM-only PoC messages, not a versioned public Celeborn wire protocol. */
private[celeborn] object RetainedShuffleControl {
  val EndpointName = "RetainedShuffleControlV1"

  sealed trait Request extends Serializable { def incarnation: String }
  case class Acquire(incarnation: String, shuffleId: Int, ttlMillis: Long) extends Request
  case class Renew(incarnation: String, lease: RetainedShuffleLease, ttlMillis: Long) extends Request
  case class Release(incarnation: String, lease: RetainedShuffleLease) extends Request
  case class Probe(incarnation: String) extends Request
  case class Reserve(
      incarnation: String,
      producer: UUID,
      appShuffleId: Int,
      numMappers: Int,
      numReducers: Int) extends Request
  case class Retire(incarnation: String, reservation: Reservation) extends Request
  case class Reservation(
      incarnation: String,
      applicationId: String,
      producer: UUID,
      appShuffleId: Int,
      shuffleId: Int,
      numMappers: Int,
      numReducers: Int,
      user: UserIdentifier) extends Serializable
  case class Seal(
      incarnation: String,
      lease: RetainedShuffleLease,
      expectedAttempts: Vector[Int],
      reducerCount: Int) extends Request
  case class Response(
      incarnation: String,
      accepted: Boolean,
      lease: Option[RetainedShuffleLease],
      reason: String,
      seal: Option[RetainedShuffleSeal] = None,
      reservation: Option[Reservation] = None) extends Serializable
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
          case Reserve(_, producer, appShuffleId, numMappers, numReducers) =>
            val reservation = service.reserve(producer, appShuffleId, numMappers, numReducers)
            Response(service.incarnation, reservation.nonEmpty, None,
              if (reservation.nonEmpty) "reserved" else "reservation rejected",
              reservation = reservation)
          case Retire(_, reservation) =>
            val retired = service.retire(reservation)
            Response(service.incarnation, retired, None,
              if (retired) "retired" else "reservation unknown")
          case Seal(_, lease, attempts, reducerCount) =>
            val seal = service.seal(lease, attempts, reducerCount)
            Response(service.incarnation, seal.nonEmpty, None,
              if (seal.nonEmpty) "sealed" else "lease or committed winner metadata unavailable", seal)
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

  def reserve(
      producer: UUID,
      appShuffleId: Int,
      numMappers: Int,
      numReducers: Int): Option[Reservation] = {
    val response = request(Reserve(incarnation, producer, appShuffleId, numMappers, numReducers))
    if (!response.accepted) None
    else {
      require(response.reservation.exists { value =>
        value.incarnation == incarnation && value.producer == producer &&
          value.appShuffleId == appShuffleId && value.numMappers == numMappers &&
          value.numReducers == numReducers && value.shuffleId >= 0 &&
          value.applicationId != null && value.applicationId.nonEmpty && value.user != null
      }, "reservation response does not match the producer shuffle")
      response.reservation
    }
  }

  def retire(reservation: Reservation): Boolean = {
    request(Retire(incarnation, reservation)).accepted
  }

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
  def seal(
      lease: RetainedShuffleLease,
      expectedAttempts: Vector[Int],
      reducerCount: Int): Option[RetainedShuffleSeal] = {
    require(lease != null)
    val response = request(Seal(incarnation, lease, expectedAttempts, reducerCount))
    if (!response.accepted) None
    else {
      require(response.seal.exists { value =>
        value.incarnation == incarnation && value.shuffleId == lease.shuffleId &&
          value.reducerCount == reducerCount && value.mapperAttempts == expectedAttempts &&
          RetainedShuffleSeal.valid(value)
      }, "seal response does not match the requested committed shuffle")
      response.seal
    }
  }

  def release(lease: RetainedShuffleLease): Unit = {
    require(request(Release(incarnation, lease)).accepted, "retention release was rejected")
  }
}
