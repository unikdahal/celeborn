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

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.{AbstractSource, Role}
import org.apache.celeborn.common.protocol.message.StatusCode

class MasterSource(conf: CelebornConf) extends AbstractSource(conf, Role.MASTER) {
  override val sourceName = "master"

  import MasterSource._
  RequestSlotsFailureStatuses.foreach { status =>
    addCounter(REQUEST_SLOTS_FAILED_COUNT, Map(STATUS_CODE_LABEL -> status.name()))
  }
  RecoveryPublishOutcomes.foreach { outcome =>
    addCounter(RECOVERY_TASK_COMMIT_PUBLISH_COUNT, Map(RECOVERY_OUTCOME_LABEL -> outcome))
    addCounter(RECOVERY_CATALOG_PUBLISH_COUNT, Map(RECOVERY_OUTCOME_LABEL -> outcome))
    addCounter(RECOVERY_ANCHOR_RESOLVE_COUNT, Map(RECOVERY_OUTCOME_LABEL -> outcome))
    addCounter(APPLICATION_LEASE_COUNT, Map(RECOVERY_OUTCOME_LABEL -> outcome))
  }
  RecoveryLookupOutcomes.foreach { outcome =>
    addCounter(RECOVERY_LOOKUP_COUNT, Map(RECOVERY_OUTCOME_LABEL -> outcome))
  }
  addCounter(RECOVERY_TASK_COMMIT_BYTES)
  // add timers
  addTimer(OFFER_SLOTS_TIME)
  addTimer(UPDATE_RESOURCE_CONSUMPTION_TIME)
  // start cleaner
  startCleaner()

  /**
   * Records the outcome of one recovery operation. Outcomes are a closed set so that a
   *  dashboard can sum them without discovering new label values at runtime.
   */
  def incRecovery(name: String, outcome: String, count: Long = 1L): Unit = {
    incCounter(name, count, Map(RECOVERY_OUTCOME_LABEL -> outcome))
  }

  def incRequestSlotsFailed(status: StatusCode): Unit = {
    if (RequestSlotsFailureStatuses.contains(status)) {
      incCounter(REQUEST_SLOTS_FAILED_COUNT, 1, Map(STATUS_CODE_LABEL -> status.name()))
    }
  }
}

object MasterSource {
  val STATUS_CODE_LABEL = "statusCode"

  val RECOVERY_OUTCOME_LABEL = "outcome"

  // Publication outcomes. "accepted" means this attempt's value became canonical; "duplicate"
  // means an earlier value was already canonical and was returned unchanged; "fenced" means the
  // caller no longer holds the application lease; "rejected" covers validation and capacity.
  val RECOVERY_OUTCOME_ACCEPTED = "accepted"
  val RECOVERY_OUTCOME_DUPLICATE = "duplicate"
  val RECOVERY_OUTCOME_FENCED = "fenced"
  val RECOVERY_OUTCOME_REJECTED = "rejected"

  // Lookup outcomes. "miss" is an authoritative absence, "corrupt" is a digest or parse failure,
  // and the two must never be conflated: a miss permits recomputation, a corrupt read does not.
  val RECOVERY_OUTCOME_HIT = "hit"
  val RECOVERY_OUTCOME_MISS = "miss"
  val RECOVERY_OUTCOME_CORRUPT = "corrupt"

  val RecoveryPublishOutcomes: Seq[String] = Seq(
    RECOVERY_OUTCOME_ACCEPTED,
    RECOVERY_OUTCOME_DUPLICATE,
    RECOVERY_OUTCOME_FENCED,
    RECOVERY_OUTCOME_REJECTED)

  val RecoveryLookupOutcomes: Seq[String] = Seq(
    RECOVERY_OUTCOME_HIT,
    RECOVERY_OUTCOME_MISS,
    RECOVERY_OUTCOME_CORRUPT,
    RECOVERY_OUTCOME_FENCED,
    RECOVERY_OUTCOME_REJECTED)

  val RECOVERY_TASK_COMMIT_PUBLISH_COUNT = "RecoveryTaskCommitPublishCount"
  val RECOVERY_TASK_COMMIT_BYTES = "RecoveryTaskCommitBytes"
  val RECOVERY_CATALOG_PUBLISH_COUNT = "RecoveryCatalogPublishCount"
  val RECOVERY_ANCHOR_RESOLVE_COUNT = "RecoveryAnchorResolveCount"
  val RECOVERY_LOOKUP_COUNT = "RecoveryLookupCount"
  val APPLICATION_LEASE_COUNT = "ApplicationLeaseCount"

  val RECOVERY_TASK_COMMIT_INLINE_BYTES = "RecoveryTaskCommitInlineBytes"
  val RECOVERY_TASK_COMMIT_INLINE_RECORDS = "RecoveryTaskCommitInlineRecords"
  val RECOVERY_COMMITTED_CATALOG_COUNT = "RecoveryCommittedCatalogCount"
  val APPLICATION_LEASE_ACTIVE_COUNT = "ApplicationLeaseActiveCount"

  val WORKER_COUNT = "WorkerCount"

  val LOST_WORKER_COUNT = "LostWorkerCount"

  val EXCLUDED_WORKER_COUNT = "ExcludedWorkerCount"

  val SHUTDOWN_WORKER_COUNT = "ShutdownWorkerCount"

  val AVAILABLE_WORKER_COUNT = "AvailableWorkerCount"

  val DECOMMISSION_WORKER_COUNT = "DecommissionWorkerCount"

  val REGISTERED_SHUFFLE_COUNT = "RegisteredShuffleCount"
  val SHUFFLE_FALLBACK_COUNT = "ShuffleFallbackCount"
  // The total count including RegisteredShuffleCount(celeborn shuffle) and ShuffleFallbackCount(engine built-in shuffle).
  val SHUFFLE_TOTAL_COUNT = "ShuffleTotalCount"

  val RUNNING_APPLICATION_COUNT = "RunningApplicationCount"
  val APPLICATION_FALLBACK_COUNT = "ApplicationFallbackCount"
  // The total count including RunningApplicationCount(celeborn shuffle) and ApplicationFallbackCount(engine built-in shuffle).
  val APPLICATION_TOTAL_COUNT = "ApplicationTotalCount"

  val IS_ACTIVE_MASTER = "IsActiveMaster"

  val PARTITION_SIZE = "PartitionSize"

  val ACTIVE_SHUFFLE_SIZE = "ActiveShuffleSize"

  val ACTIVE_SHUFFLE_FILE_COUNT = "ActiveShuffleFileCount"

  val OFFER_SLOTS_TIME = "OfferSlotsTime"
  val REQUEST_SLOTS_FAILED_COUNT = "RequestSlotsFailed"

  val RATIS_APPLY_COMPLETED_INDEX = "RatisApplyCompletedIndex"

  // Capacity
  val DEVICE_CELEBORN_FREE_CAPACITY = "DeviceCelebornFreeBytes"
  val DEVICE_CELEBORN_TOTAL_CAPACITY = "DeviceCelebornTotalBytes"

  val UPDATE_RESOURCE_CONSUMPTION_TIME = "UpdateResourceConsumptionTime"

  private val RequestSlotsFailureStatuses =
    Seq(StatusCode.SLOT_NOT_AVAILABLE, StatusCode.WORKER_EXCLUDED)
}
