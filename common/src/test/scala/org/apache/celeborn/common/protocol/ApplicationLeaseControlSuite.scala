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

package org.apache.celeborn.common.protocol

import java.util.Collections

import com.google.protobuf.ByteString
import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol.message.ControlMessages
import org.apache.celeborn.common.protocol.message.ControlMessages.{CommitFiles, DestroyWorkerSlots, FenceApplication, FenceApplicationResponse}

class ApplicationLeaseControlSuite extends CelebornFunSuite {

  test("application lease control messages round-trip through both transport decoders") {
    val request = PbApplicationLeaseControl.newBuilder()
      .setAppId("logical-app")
      .setExpectedEpoch(7L)
      .setOwnerId("driver-2")
      .setExpiresAtMs(9000L)
      .setLeaseDurationMs(60000L)
      .setRenewal(true)
      .setRequestId("00000000-0000-0000-0000-000000000001#3")
      .build()
    val encoded = ControlMessages.toTransportMessage(request)

    assert(encoded.getType == MessageType.APPLICATION_LEASE_CONTROL)
    assert(ControlMessages.fromTransportMessage(encoded) == request)
    assert(
      TransportMessage.fromByteBuffer(encoded.toByteBuffer)
        .getParsedPayload[PbApplicationLeaseControl] == request)

    val response = PbApplicationLeaseControlResponse.newBuilder()
      .setSuccess(true)
      .setEpoch(7L)
      .setOwnerId("driver-2")
      .setExpiresAtMs(9000L)
      .build()
    val encodedResponse = ControlMessages.toTransportMessage(response)
    assert(ControlMessages.fromTransportMessage(encodedResponse) == response)
    assert(
      TransportMessage.fromByteBuffer(encodedResponse.toByteBuffer)
        .getParsedPayload[PbApplicationLeaseControlResponse] == response)
  }

  test("worker fence and mutation lease tokens round-trip") {
    val fence = FenceApplication("logical-app", 8L, "driver-3", 10000L, 5000L)
    val encodedFence = ControlMessages.toTransportMessage(fence)
    assert(encodedFence.getType == MessageType.FENCE_APPLICATION)
    assert(ControlMessages.fromTransportMessage(encodedFence) == fence)
    assert(ControlMessages.fromTransportMessage(
      ControlMessages.toTransportMessage(FenceApplicationResponse(success = true))) ==
      FenceApplicationResponse(success = true))

    val commit = CommitFiles(
      "logical-app",
      3,
      Collections.singletonList("primary"),
      Collections.emptyList(),
      Array(0),
      11L,
      applicationLeaseEpoch = 8L,
      applicationLeaseOwnerId = "driver-3")
    val decodedCommit = ControlMessages.fromTransportMessage(
      ControlMessages.toTransportMessage(commit)).asInstanceOf[CommitFiles]
    assert(decodedCommit.copy(mapAttempts = commit.mapAttempts) == commit)

    val destroy = DestroyWorkerSlots(
      "logical-app-3",
      Collections.singletonList("primary"),
      Collections.emptyList(),
      applicationLeaseEpoch = 8L,
      applicationLeaseOwnerId = "driver-3")
    assert(
      ControlMessages.fromTransportMessage(ControlMessages.toTransportMessage(destroy)) == destroy)
  }

  test("committed shuffle catalog RPCs round-trip through transport") {
    val catalog = ByteString.copyFromUtf8("catalog")
    val publish = PbPublishCommittedShuffleCatalog.newBuilder()
      .setAppId("logical-app")
      .setShuffleId(3)
      .setCatalog(catalog)
      .setApplicationLeaseEpoch(8L)
      .setApplicationLeaseOwnerId("driver-3")
      .setRequestId("00000000-0000-0000-0000-000000000001#4")
      .build()
    val publishResponse = PbPublishCommittedShuffleCatalogResponse.newBuilder()
      .setSuccess(true)
      .setSha256(ByteString.copyFrom(new Array[Byte](32)))
      .build()
    val get = PbGetCommittedShuffleCatalog.newBuilder()
      .setAppId("logical-app")
      .setShuffleId(3)
      .setRecoveryKey("query-1/stage-1")
      .setApplicationLeaseEpoch(8L)
      .setApplicationLeaseOwnerId("driver-3")
      .build()
    val getResponse = PbGetCommittedShuffleCatalogResponse.newBuilder()
      .setSuccess(true)
      .setFound(true)
      .setCatalog(catalog)
      .setSha256(ByteString.copyFrom(new Array[Byte](32)))
      .build()

    Seq(publish, publishResponse, get, getResponse).foreach { message =>
      val encoded = ControlMessages.toTransportMessage(message)
      assert(ControlMessages.fromTransportMessage(encoded) == message)
      assert(TransportMessage.fromByteBuffer(encoded.toByteBuffer)
        .getParsedPayload[com.google.protobuf.GeneratedMessageV3] == message)
    }
  }

  test("source recovery anchor RPCs round-trip through both transport decoders") {
    val request = PbResolveSourceRecoveryAnchor.newBuilder()
      .setAppId("logical-app")
      .setRecoveryId("query-1")
      .setSourceId("iceberg:catalog.db.table")
      .setCurrentAnchor("snapshot:42")
      .setApplicationLeaseEpoch(8L)
      .setApplicationLeaseOwnerId("driver-3")
      .setRequestId("00000000-0000-0000-0000-000000000001#5")
      .build()
    val response = PbResolveSourceRecoveryAnchorResponse.newBuilder()
      .setSuccess(true)
      .setAnchor("snapshot:41")
      .build()

    Seq(request, response).foreach { message =>
      val encoded = ControlMessages.toTransportMessage(message)
      assert(ControlMessages.fromTransportMessage(encoded) == message)
      assert(TransportMessage.fromByteBuffer(encoded.toByteBuffer)
        .getParsedPayload[com.google.protobuf.GeneratedMessageV3] == message)
    }
  }

  test("recovery task commit RPCs round-trip through both transport decoders") {
    val payload = ByteString.copyFromUtf8("task-envelope")
    val digest = ByteString.copyFrom(new Array[Byte](32))
    val messages = Seq(
      PbPublishRecoveryTaskCommit.newBuilder()
        .setAppId("logical-app").setRecoveryId("query-1").setWriteId("write-1")
        .setPartitionId(3).setPayload(payload).setSha256(digest)
        .setApplicationLeaseEpoch(8L).setApplicationLeaseOwnerId("driver-3").build(),
      PbPublishRecoveryTaskCommitResponse.newBuilder()
        .setSuccess(true).setPayload(payload).setSha256(digest).build(),
      PbGetRecoveryTaskCommit.newBuilder()
        .setAppId("logical-app").setRecoveryId("query-1").setWriteId("write-1")
        .setPartitionId(3).setApplicationLeaseEpoch(8L)
        .setApplicationLeaseOwnerId("driver-3").build(),
      PbGetRecoveryTaskCommitResponse.newBuilder()
        .setSuccess(true).setFound(true).setPayload(payload).setSha256(digest).build(),
      PbBatchGetRecoveryTaskCommits.newBuilder()
        .setAppId("logical-app").setRecoveryId("query-1").setWriteId("write-1")
        .addPartitionIds(-1).addPartitionIds(3).setApplicationLeaseEpoch(8L)
        .setApplicationLeaseOwnerId("driver-3").build(),
      PbBatchGetRecoveryTaskCommitsResponse.newBuilder().setSuccess(true)
        .addEntries(PbRecoveryTaskCommitEntry.newBuilder().setPartitionId(-1))
        .addEntries(PbRecoveryTaskCommitEntry.newBuilder().setPartitionId(3)
          .setFound(true).setPayload(payload).setSha256(digest)).build())

    messages.foreach { message =>
      val encoded = ControlMessages.toTransportMessage(message)
      assert(ControlMessages.fromTransportMessage(encoded) == message)
      assert(TransportMessage.fromByteBuffer(encoded.toByteBuffer)
        .getParsedPayload[com.google.protobuf.GeneratedMessageV3] == message)
    }
  }
}
