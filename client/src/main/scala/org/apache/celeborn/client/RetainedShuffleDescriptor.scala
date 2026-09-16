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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.celeborn.common.identity.UserIdentifier

/** Bounded provider bytes for a durable Spark manifest; contains no transferable lease token. */
private[celeborn] final case class RetainedShuffleDescriptor(
    host: String,
    port: Int,
    user: UserIdentifier,
    seal: RetainedShuffleSeal)

private[celeborn] object RetainedShuffleDescriptor {
  private val Magic = 0x43524431 // CRD1
  private val Version = 1
  private val MaxBytes = 5 * 1024 * 1024

  private def text(value: String): String = {
    require(value != null && value.nonEmpty && value.length <= 1024,
      "invalid retained descriptor text")
    value
  }

  def encode(value: RetainedShuffleDescriptor): Array[Byte] = {
    require(value != null && value.user != null && RetainedShuffleSeal.valid(value.seal))
    require(value.port > 0 && value.port <= 65535)
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    out.writeInt(Magic)
    out.writeInt(Version)
    out.writeUTF(text(value.host))
    out.writeInt(value.port)
    out.writeUTF(text(value.user.tenantId))
    out.writeUTF(text(value.user.name))
    val seal = value.seal
    out.writeUTF(text(seal.applicationId))
    out.writeUTF(text(seal.incarnation))
    out.writeInt(seal.shuffleId)
    out.writeInt(seal.reducerCount)
    out.writeInt(seal.mapperAttempts.size)
    seal.mapperAttempts.foreach(out.writeInt)
    out.writeInt(seal.nativeReadMetadata.size)
    out.write(seal.nativeReadMetadata.toArray)
    out.write(seal.digest.toArray)
    out.flush()
    require(bytes.size() <= MaxBytes, "retained descriptor is too large")
    bytes.toByteArray
  }

  def decode(bytes: Array[Byte]): RetainedShuffleDescriptor = {
    require(bytes != null && bytes.nonEmpty && bytes.length <= MaxBytes)
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    require(in.readInt() == Magic && in.readInt() == Version,
      "unsupported retained descriptor format")
    val host = text(in.readUTF())
    val port = in.readInt()
    require(port > 0 && port <= 65535)
    val user = UserIdentifier(text(in.readUTF()), text(in.readUTF()))
    val applicationId = text(in.readUTF())
    val incarnation = text(in.readUTF())
    val shuffleId = in.readInt()
    val reducerCount = in.readInt()
    val mapperCount = in.readInt()
    require(mapperCount > 0 && mapperCount <= 65536 && mapperCount <= in.available() / 4)
    val attempts = Vector.fill(mapperCount)(in.readInt())
    val payloadSize = in.readInt()
    require(payloadSize > 0 && payloadSize <= 4 * 1024 * 1024 &&
      payloadSize <= in.available() - 32, "invalid retained metadata length")
    val payload = new Array[Byte](payloadSize)
    in.readFully(payload)
    val digest = new Array[Byte](32)
    in.readFully(digest)
    require(in.available() == 0, "trailing retained descriptor bytes")
    val seal = RetainedShuffleSeal(applicationId, incarnation, shuffleId, reducerCount,
      attempts, payload.toVector, digest.toVector)
    require(RetainedShuffleSeal.valid(seal), "retained descriptor seal is invalid")
    RetainedShuffleDescriptor(host, port, user, seal)
  }
}
