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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** Native read snapshot after commit and Spark's accepted mapper-attempt vector agree. */
private[celeborn] final case class RetainedShuffleSeal(
    applicationId: String,
    incarnation: String,
    shuffleId: Int,
    reducerCount: Int,
    mapperAttempts: Vector[Int],
    nativeReadMetadata: Vector[Byte],
    digest: Vector[Byte]) extends Serializable

private[celeborn] object RetainedShuffleSeal {
  val FormatVersion = 1

  def create(
      applicationId: String,
      incarnation: String,
      shuffleId: Int,
      reducerCount: Int,
      mapperAttempts: Vector[Int],
      nativeReadMetadata: Vector[Byte]): RetainedShuffleSeal = {
    require(applicationId != null && applicationId.nonEmpty && applicationId.length <= 1024)
    require(incarnation != null && incarnation.nonEmpty && incarnation.length <= 128)
    require(shuffleId >= 0 && reducerCount > 0 && reducerCount <= 65536)
    require(mapperAttempts != null && mapperAttempts.nonEmpty && mapperAttempts.size <= 65536)
    require(mapperAttempts.forall(_ >= 0))
    require(nativeReadMetadata != null && nativeReadMetadata.nonEmpty &&
      nativeReadMetadata.size <= 4 * 1024 * 1024)
    val digest = MessageDigest.getInstance("SHA-256")
    val application = applicationId.getBytes(StandardCharsets.UTF_8)
    val owner = incarnation.getBytes(StandardCharsets.UTF_8)
    digest.update(ByteBuffer.allocate(20).putInt(FormatVersion).putInt(owner.length)
      .putInt(shuffleId).putInt(reducerCount).putInt(mapperAttempts.size).array())
    digest.update(owner)
    digest.update(ByteBuffer.allocate(4).putInt(application.length).array())
    digest.update(application)
    mapperAttempts.foreach { attempt =>
      digest.update(ByteBuffer.allocate(4).putInt(attempt).array())
    }
    digest.update(ByteBuffer.allocate(4).putInt(nativeReadMetadata.size).array())
    digest.update(nativeReadMetadata.toArray)
    RetainedShuffleSeal(applicationId, incarnation, shuffleId, reducerCount, mapperAttempts,
      nativeReadMetadata, digest.digest().toVector)
  }

  def valid(seal: RetainedShuffleSeal): Boolean = {
    if (seal == null) false
    else {
      try {
        create(seal.applicationId, seal.incarnation, seal.shuffleId, seal.reducerCount,
          seal.mapperAttempts, seal.nativeReadMetadata).digest == seal.digest
      } catch {
        case _: IllegalArgumentException => false
      }
    }
  }
}
