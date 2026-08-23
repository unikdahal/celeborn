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

package org.apache.celeborn.service.deploy.worker

import java.util.Arrays

import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{FileInfo, ReduceFileMeta}
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.service.deploy.worker.storage.StorageManager

class ControllerSuite extends AnyFunSuite {

  private val applicationId = "application-1"
  private val shuffleId = 7
  private val fileName = "0-0-0"

  private def newController(storageManager: StorageManager): Controller = {
    val controller = new Controller(null, new CelebornConf(), null, null)
    controller.storageManager = storageManager
    controller
  }

  private def descriptor(
      fileSize: Long = 12L,
      chunkOffsets: java.util.List[java.lang.Long] = Arrays.asList(0L, 12L)) = {
    ShuffleFileDescriptor(fileName, fileSize, chunkOffsets)
  }

  test("validate exact committed shuffle files") {
    val storageManager = mock[StorageManager]
    val fileInfo = mock[FileInfo]
    when(storageManager.getFileInfo(s"$applicationId-$shuffleId", fileName)).thenReturn(fileInfo)
    when(fileInfo.getFileLength).thenReturn(12L)
    when(fileInfo.isReduceFileMeta).thenReturn(true)
    when(fileInfo.getReduceFileMeta).thenReturn(new ReduceFileMeta(
      Arrays.asList(Long.box(0L), Long.box(12L))))

    val response = newController(storageManager).validateShuffleFiles(
      applicationId,
      shuffleId,
      Arrays.asList(descriptor()))

    assert(response.status == StatusCode.SUCCESS)
  }

  test("reject missing or truncated committed shuffle files") {
    val storageManager = mock[StorageManager]
    when(storageManager.getFileInfo(s"$applicationId-$shuffleId", fileName))
      .thenReturn(null)
    val controller = newController(storageManager)
    val missing = controller.validateShuffleFiles(
      applicationId,
      shuffleId,
      Arrays.asList(descriptor()))
    assert(missing.status == StatusCode.REQUEST_FAILED)
    assert(missing.reason.contains("missing file"))

    val fileInfo = mock[FileInfo]
    when(storageManager.getFileInfo(s"$applicationId-$shuffleId", fileName)).thenReturn(fileInfo)
    when(fileInfo.getFileLength).thenReturn(11L)
    val truncated = controller.validateShuffleFiles(
      applicationId,
      shuffleId,
      Arrays.asList(descriptor()))
    assert(truncated.status == StatusCode.REQUEST_FAILED)
    assert(truncated.reason.contains("length 11"))
  }

  test("reject mismatched committed chunk metadata") {
    val storageManager = mock[StorageManager]
    val fileInfo = mock[FileInfo]
    when(storageManager.getFileInfo(s"$applicationId-$shuffleId", fileName)).thenReturn(fileInfo)
    when(fileInfo.getFileLength).thenReturn(12L)
    when(fileInfo.isReduceFileMeta).thenReturn(true)
    when(fileInfo.getReduceFileMeta).thenReturn(new ReduceFileMeta(
      Arrays.asList(Long.box(0L), Long.box(6L), Long.box(12L))))

    val response = newController(storageManager).validateShuffleFiles(
      applicationId,
      shuffleId,
      Arrays.asList(descriptor()))

    assert(response.status == StatusCode.REQUEST_FAILED)
    assert(response.reason.contains("different chunk offsets"))
  }

  test("reject empty, duplicate, and malformed validation anchors") {
    val controller = newController(mock[StorageManager])
    assert(controller.validateShuffleFiles(
      applicationId,
      shuffleId,
      java.util.Collections.emptyList()).status == StatusCode.REQUEST_FAILED)
    assert(controller.validateShuffleFiles(
      applicationId,
      shuffleId,
      Arrays.asList(descriptor(), descriptor())).reason.contains("duplicate file"))
    assert(controller.validateShuffleFiles(
      applicationId,
      shuffleId,
      Arrays.asList(descriptor(chunkOffsets = Arrays.asList(1L, 12L))))
      .reason.contains("invalid expected chunk offsets"))
  }
}
