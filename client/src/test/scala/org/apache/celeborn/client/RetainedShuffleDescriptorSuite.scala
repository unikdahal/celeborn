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

import java.io.EOFException

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.identity.UserIdentifier

class RetainedShuffleDescriptorSuite extends CelebornFunSuite {
  private def descriptor: RetainedShuffleDescriptor = {
    val seal = RetainedShuffleSeal.create("owner-app", "incarnation", 17, 2,
      Vector(0, 3), Vector[Byte](1, 2, 3))
    RetainedShuffleDescriptor("localhost", 19000, UserIdentifier("tenant", "user"), seal)
  }

  test("descriptor round trip preserves owner, native namespace and accepted winners") {
    val original = descriptor
    val encoded = RetainedShuffleDescriptor.encode(original)
    assert(RetainedShuffleDescriptor.decode(encoded) == original)
    // A decoded descriptor owns its payload; changing the input cannot change the seal.
    val decoded = RetainedShuffleDescriptor.decode(encoded)
    java.util.Arrays.fill(encoded, 0.toByte)
    assert(decoded == original)
  }

  test("descriptor rejects corruption, trailing bytes and truncation") {
    val encoded = RetainedShuffleDescriptor.encode(descriptor)
    val corrupted = encoded.clone()
    corrupted(corrupted.length - 1) = (corrupted.last ^ 1).toByte
    intercept[IllegalArgumentException] {
      RetainedShuffleDescriptor.decode(corrupted)
    }
    intercept[IllegalArgumentException] {
      RetainedShuffleDescriptor.decode(encoded ++ Array[Byte](0))
    }
    intercept[IllegalArgumentException] {
      RetainedShuffleDescriptor.decode(encoded.dropRight(1))
    }
    intercept[EOFException] {
      RetainedShuffleDescriptor.decode(encoded.take(3))
    }
  }

  test("a seal cannot move between application namespaces or mapper winner vectors") {
    val original = descriptor
    intercept[IllegalArgumentException] {
      RetainedShuffleDescriptor.encode(original.copy(
        seal = original.seal.copy(applicationId = "replacement-app")))
    }
    intercept[IllegalArgumentException] {
      RetainedShuffleDescriptor.encode(original.copy(
        seal = original.seal.copy(mapperAttempts = Vector(1, 3))))
    }
  }
}
