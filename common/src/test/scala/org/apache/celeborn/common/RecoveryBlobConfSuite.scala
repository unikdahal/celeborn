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

package org.apache.celeborn.common

import org.scalatest.funsuite.AnyFunSuite

class RecoveryBlobConfSuite extends AnyFunSuite {

  test("the blob backend is off by default and its defaults are internally consistent") {
    val conf = new CelebornConf()

    assert(!conf.recoveryBlobEnabled, "an unconfigured cluster keeps the inline backend")
    assert(conf.recoveryBlobReplicationFactor == 3)
    assert(conf.recoveryBlobQuorum == 2)
    assert(
      conf.recoveryBlobQuorum <= conf.recoveryBlobReplicationFactor,
      "a quorum larger than the replica count could never be reached")
    assert(conf.recoveryBlobInlineThreshold == 4096L)
    assert(conf.recoveryBlobOrphanGrace == 3600000L)
    assert(conf.recoveryBlobRepairInterval == 300000L)
  }

  test("a quorum larger than the replication factor is rejected rather than deadlocking") {
    val conf = new CelebornConf()
      .set(CelebornConf.RECOVERY_BLOB_REPLICATION_FACTOR.key, "2")
      .set(CelebornConf.RECOVERY_BLOB_QUORUM.key, "3")

    val error = intercept[IllegalArgumentException](conf.recoveryBlobQuorum)
    assert(error.getMessage.contains("cannot exceed"))
  }

  test("non-positive replication, quorum, grace and interval are refused") {
    intercept[IllegalArgumentException] {
      new CelebornConf().set(CelebornConf.RECOVERY_BLOB_REPLICATION_FACTOR.key, "0")
        .recoveryBlobReplicationFactor
    }
    intercept[IllegalArgumentException] {
      new CelebornConf().set(CelebornConf.RECOVERY_BLOB_QUORUM.key, "0").recoveryBlobQuorum
    }
    intercept[IllegalArgumentException] {
      new CelebornConf().set(CelebornConf.RECOVERY_BLOB_ORPHAN_GRACE.key, "0s")
        .recoveryBlobOrphanGrace
    }
    intercept[IllegalArgumentException] {
      new CelebornConf().set(CelebornConf.RECOVERY_BLOB_REPAIR_INTERVAL.key, "0s")
        .recoveryBlobRepairInterval
    }
  }

  test("a single-replica configuration is allowed but must use a quorum of one") {
    val conf = new CelebornConf()
      .set(CelebornConf.RECOVERY_BLOB_REPLICATION_FACTOR.key, "1")
      .set(CelebornConf.RECOVERY_BLOB_QUORUM.key, "1")

    assert(conf.recoveryBlobQuorum == 1)
  }
}
