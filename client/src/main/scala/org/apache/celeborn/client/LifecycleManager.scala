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

import java.lang.{Byte => JByte}
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.security.SecureRandom
import java.util
import java.util.{function, List => JList}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, LongAdder}
import java.util.function.{BiConsumer, BiFunction, Consumer}

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random

import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.{Cache, CacheBuilder}
import org.roaringbitmap.RoaringBitmap

import org.apache.celeborn.client.LifecycleManager.{ShuffleAllocatedWorkers, ShuffleFailedWorkers}
import org.apache.celeborn.client.listener.WorkerStatusListener
import org.apache.celeborn.common.{CelebornConf, CommitMetadata}
import org.apache.celeborn.common.CelebornConf.ACTIVE_STORAGE_TYPES
import org.apache.celeborn.common.client.{ApplicationInfoProvider, MasterClient}
import org.apache.celeborn.common.identity.{IdentityProvider, UserIdentifier}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{ApplicationLease, ApplicationMeta, ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.network.protocol.{SerdeVersion, TransportMessagesHelper}
import org.apache.celeborn.common.network.sasl.registration.RegistrationInfo
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.RpcNameConstants.WORKER_EP
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.rpc.{ClientSaslContextBuilder, RpcSecurityContext, RpcSecurityContextBuilder}
import org.apache.celeborn.common.rpc.netty.{LocalNettyRpcCallContext, RemoteNettyRpcCallContext}
import org.apache.celeborn.common.util.{JavaUtils, PbSerDeUtils, RecoveryTaskCommitUtils, ThreadUtils, Utils}
// Can Remove this if celeborn don't support scala211 in future
import org.apache.celeborn.common.util.FunctionConverter._
import org.apache.celeborn.common.util.ThreadUtils.awaitResult
import org.apache.celeborn.common.util.Utils.UNKNOWN_APP_SHUFFLE_ID
import org.apache.celeborn.common.write.LocationPushFailedBatches

object LifecycleManager {
  // shuffle id -> partition id -> partition locations
  type ShuffleFileGroups =
    ConcurrentHashMap[Int, ConcurrentHashMap[Integer, util.Set[PartitionLocation]]]
  // shuffle id -> partition uniqueId -> PushFailedBatch set
  type ShufflePushFailedBatches =
    ConcurrentHashMap[Int, util.HashMap[String, LocationPushFailedBatches]]
  type ShuffleAllocatedWorkers =
    ConcurrentHashMap[Int, ConcurrentHashMap[String, ShufflePartitionLocationInfo]]
  type ShuffleFailedWorkers = ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]

  /** Spark-visible statistics for a shuffle adopted from Celeborn's committed catalog. */
  case class AdoptedShuffleCatalog(
      celebornShuffleId: Int,
      bytesByPartitionId: Array[Long],
      catalogSha256: Array[Byte])

  private[client] def sourceRecoveryBindingId(sourceId: String): String =
    s"source:v1:${sourceId.length}:$sourceId"

  private[client] def writeRecoveryBindingId(sinkId: String): String =
    s"write:v1:${sinkId.length}:$sinkId"
}

class LifecycleManager(val appUniqueId: String, val conf: CelebornConf) extends RpcEndpoint
  with Logging {

  private val lifecycleHost = Utils.localHostName(conf)

  private val shuffleExpiredCheckIntervalMs = conf.shuffleExpiredCheckIntervalMs
  private val slotsAssignMaxWorkers = conf.clientSlotAssignMaxWorkers
  private val pushReplicateEnabled = conf.clientPushReplicateEnabled
  private val pushRackAwareEnabled = conf.clientReserveSlotsRackAwareEnabled
  private val partitionSplitThreshold = conf.shufflePartitionSplitThreshold
  private val partitionSplitMode = conf.shufflePartitionSplitMode
  // shuffle id -> partition type
  private val shufflePartitionType = JavaUtils.newConcurrentHashMap[Int, PartitionType]()
  private val rangeReadFilter = conf.shuffleRangeReadFilterEnabled
  private val unregisterShuffleTime = JavaUtils.newConcurrentHashMap[Int, Long]()
  private val shuffleRecoveryKeys = JavaUtils.newConcurrentHashMap[Int, String]()

  val registeredShuffle = ConcurrentHashMap.newKeySet[Int]()
  val shuffleCount = new LongAdder()
  val applicationCount = new LongAdder()
  val shuffleFallbackCounts = JavaUtils.newConcurrentHashMap[String, java.lang.Long]()
  val applicationFallbackCounts = JavaUtils.newConcurrentHashMap[String, java.lang.Long]()
  // maintain each shuffle's map relation of WorkerInfo and partition location
  val shuffleAllocatedWorkers = new ShuffleAllocatedWorkers
  // shuffle id -> (partitionId -> newest PartitionLocation)
  val latestPartitionLocation =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Int, PartitionLocation]]()
  private val userIdentifier: UserIdentifier = IdentityProvider.instantiate(conf).provide()
  private val availableStorageTypes = conf.availableStorageTypes
  private val storageTypes =
    conf.get(ACTIVE_STORAGE_TYPES).split(",").map(StorageInfo.Type.valueOf).toList
  // app shuffle id -> LinkedHashMap of (app shuffle identifier, (shuffle id, fetch status))
  private val shuffleIdMapping = JavaUtils.newConcurrentHashMap[
    Int,
    scala.collection.mutable.LinkedHashMap[String, (Int, Boolean)]]()
  private val celebornShuffleIdToAppShuffleIdMap = JavaUtils.newConcurrentHashMap[Int, Int]()
  private val shuffleIdGenerator = new AtomicInteger(0)
  // app shuffle id -> whether shuffle is determinate, rerun of a indeterminate shuffle gets different result
  private val appShuffleDeterminateMap = JavaUtils.newConcurrentHashMap[Int, Boolean]();

  private val rpcCacheSize = conf.clientRpcCacheSize
  private val rpcCacheConcurrencyLevel = conf.clientRpcCacheConcurrencyLevel
  private val rpcCacheExpireTime = conf.clientRpcCacheExpireTime
  private val rpcMaxRetires = conf.clientRpcMaxRetries
  private val rpcAskTimeoutMs = conf.rpcAskTimeout.duration.toMillis

  private val batchRemoveExpiredShufflesEnabled = conf.batchHandleRemoveExpiredShufflesEnabled

  private val excludedWorkersFilter = conf.registerShuffleFilterExcludedWorkerEnabled

  private val registerShuffleResponseRpcCache: Cache[Int, ByteBuffer] = CacheBuilder.newBuilder()
    .concurrencyLevel(rpcCacheConcurrencyLevel)
    .expireAfterAccess(rpcCacheExpireTime, TimeUnit.MILLISECONDS)
    .maximumSize(rpcCacheSize)
    .build().asInstanceOf[Cache[Int, ByteBuffer]]

  private val clientTagsExpr = conf.tagsExpr
  private val mockDestroyFailure = conf.testMockDestroySlotsFailure
  private val authEnabled = conf.authEnabledOnClient
  private var applicationMeta: ApplicationMeta = _
  @VisibleForTesting
  def workerSnapshots(shuffleId: Int): util.Map[String, ShufflePartitionLocationInfo] =
    shuffleAllocatedWorkers.get(shuffleId)

  @VisibleForTesting
  def getUnregisterShuffleTime(): ConcurrentHashMap[Int, Long] =
    unregisterShuffleTime

  val newMapFunc: function.Function[Int, ConcurrentHashMap[Int, PartitionLocation]] =
    new util.function.Function[Int, ConcurrentHashMap[Int, PartitionLocation]]() {
      override def apply(s: Int): ConcurrentHashMap[Int, PartitionLocation] = {
        JavaUtils.newConcurrentHashMap[Int, PartitionLocation]()
      }
    }

  def updateLatestPartitionLocations(
      shuffleId: Int,
      locations: util.List[PartitionLocation]): Unit = {
    val map = latestPartitionLocation.computeIfAbsent(shuffleId, newMapFunc)
    locations.asScala.foreach(location => map.put(location.getId, location))
    invalidateLatestMaxLocsCache(shuffleId)
  }

  case class RegisterCallContext(context: RpcCallContext, partitionId: Int = -1) {
    def reply(response: RegisterShuffleResponse) = {
      context.reply(response)
    }
  }

  // register shuffle request waiting for response
  private val registeringShuffleRequest =
    JavaUtils.newConcurrentHashMap[Int, util.Set[RegisterCallContext]]()

  // Threads
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-message-forwarder")
  private var checkForShuffleRemoval: ScheduledFuture[_] = _
  val rpcSharedThreadPool =
    ThreadUtils.newDaemonCachedThreadPool(
      "celeborn-client-lifecycle-manager-shared-rpc-pool",
      conf.clientRpcSharedThreads,
      30)
  val ec = ExecutionContext.fromExecutor(rpcSharedThreadPool)

  // init driver celeborn LifecycleManager rpc service
  override val rpcEnv: RpcEnv = RpcEnv.create(
    RpcNameConstants.LIFECYCLE_MANAGER_SYS,
    TransportModuleConstants.RPC_LIFECYCLEMANAGER_MODULE,
    lifecycleHost,
    conf.shuffleManagerPort,
    conf,
    Role.CLIENT,
    None)
  rpcEnv.setupEndpoint(RpcNameConstants.LIFECYCLE_MANAGER_EP, this)

  logInfo(s"Starting LifecycleManager on ${rpcEnv.address}")

  private var masterRpcEnvInUse = rpcEnv
  private var workerRpcEnvInUse = rpcEnv
  if (authEnabled) {
    logInfo(s"Authentication is enabled; setting up master and worker RPC environments")
    val appSecret = createSecret()
    applicationMeta = ApplicationMeta(appUniqueId, appSecret)
    val registrationInfo = new RegistrationInfo()
    masterRpcEnvInUse =
      RpcEnv.create(
        RpcNameConstants.LIFECYCLE_MANAGER_MASTER_SYS,
        TransportModuleConstants.RPC_SERVICE_MODULE,
        lifecycleHost,
        0,
        conf,
        Role.CLIENT,
        createRpcSecurityContext(
          appSecret,
          addClientRegistrationBootstrap = true,
          Some(registrationInfo)))
    workerRpcEnvInUse =
      RpcEnv.create(
        RpcNameConstants.LIFECYCLE_MANAGER_WORKER_SYS,
        TransportModuleConstants.RPC_SERVICE_MODULE,
        lifecycleHost,
        0,
        conf,
        Role.CLIENT,
        createRpcSecurityContext(appSecret))
  }

  @volatile private var applicationLease: ApplicationLease = _

  private val masterClient = new MasterClient(masterRpcEnvInUse, conf, false)
  val commitManager = new CommitManager(appUniqueId, conf, this)
  val workerStatusTracker = new WorkerStatusTracker(conf, this)
  private val heartbeater =
    new ApplicationHeartbeater(
      appUniqueId,
      conf,
      masterClient,
      () => {
        commitManager.commitMetrics() ->
          (shuffleCount.sumThenReset(), applicationCount.sumThenReset(), resetFallbackCounts(
            shuffleFallbackCounts), resetFallbackCounts(applicationFallbackCounts))
      },
      workerStatusTracker,
      registeredShuffle,
      () => applicationLease,
      reason => cancelAllActiveStages(reason))
  private def resetFallbackCounts(counts: ConcurrentHashMap[String, java.lang.Long])
      : Map[String, java.lang.Long] = {
    val fallbackCounts = new util.HashMap[String, java.lang.Long]()
    counts.keys().asScala.foreach { key =>
      Option(counts.remove(key)).filter(_ > 0).foreach(fallbackCounts.put(key, _))
    }
    fallbackCounts.asScala.toMap
  }
  private val changePartitionManager = new ChangePartitionManager(conf, this)
  private val releasePartitionManager = new ReleasePartitionManager(conf, this)

  private val messagesHelper: TransportMessagesHelper = new TransportMessagesHelper()

  private def registerApplicationInfo(): Unit = {
    Utils.tryLogNonFatalError {
      val extraInfo = ApplicationInfoProvider.instantiate(conf)
        .provide() + (CelebornConf.QUOTA_INTERRUPT_SHUFFLE_ENABLED.key -> conf.quotaInterruptShuffleEnabled.toString)
      masterClient.send(RegisterApplicationInfo(
        appUniqueId,
        userIdentifier,
        extraInfo.asJava))
    }
  }

  def acquireApplicationLease(
      expectedEpoch: Long,
      ownerId: String,
      expiresAtMs: Long): PbApplicationLeaseControlResponse = {
    requestApplicationLease(expectedEpoch, ownerId, expiresAtMs, renewal = false)
  }

  def renewApplicationLease(
      epoch: Long,
      ownerId: String,
      expiresAtMs: Long): PbApplicationLeaseControlResponse = {
    requestApplicationLease(epoch, ownerId, expiresAtMs, renewal = true)
  }

  /** Atomically acquire the next lease epoch using the master's clock. */
  def takeOverApplicationLease(
      ownerId: String,
      leaseDurationMs: Long): PbApplicationLeaseControlResponse = {
    require(ownerId != null && ownerId.nonEmpty, "ownerId must be non-empty")
    require(leaseDurationMs > 0, "leaseDurationMs must be positive")
    val response = masterClient.askSync[PbApplicationLeaseControlResponse](
      PbApplicationLeaseControl.newBuilder()
        .setAppId(appUniqueId)
        .setExpectedEpoch(-1L)
        .setOwnerId(ownerId)
        .setLeaseDurationMs(leaseDurationMs)
        .setRenewal(false)
        .setRequestId(MasterClient.genRequestId())
        .build(),
      classOf[PbApplicationLeaseControlResponse])
    if (response.getSuccess) {
      applicationLease =
        new ApplicationLease(response.getEpoch, response.getOwnerId, response.getExpiresAtMs)
    }
    response
  }

  /** Renew the current epoch using the master's clock. */
  def renewApplicationLeaseForDuration(
      epoch: Long,
      ownerId: String,
      leaseDurationMs: Long): PbApplicationLeaseControlResponse = {
    require(epoch > 0, "epoch must be positive")
    require(ownerId != null && ownerId.nonEmpty, "ownerId must be non-empty")
    require(leaseDurationMs > 0, "leaseDurationMs must be positive")
    val response = masterClient.askSync[PbApplicationLeaseControlResponse](
      PbApplicationLeaseControl.newBuilder()
        .setAppId(appUniqueId)
        .setExpectedEpoch(epoch)
        .setOwnerId(ownerId)
        .setLeaseDurationMs(leaseDurationMs)
        .setRenewal(true)
        .setRequestId(MasterClient.genRequestId())
        .build(),
      classOf[PbApplicationLeaseControlResponse])
    if (response.getSuccess) {
      applicationLease =
        new ApplicationLease(response.getEpoch, response.getOwnerId, response.getExpiresAtMs)
    }
    response
  }

  private[client] def currentApplicationLease: ApplicationLease = applicationLease

  def resolveSourceRecoveryAnchor(
      recoveryId: String,
      sourceId: String,
      currentAnchor: String): PbResolveSourceRecoveryAnchorResponse = {
    require(sourceId != null && sourceId.nonEmpty, "sourceId must be non-empty")
    require(currentAnchor != null && currentAnchor.nonEmpty, "currentAnchor must be non-empty")
    resolveRecoveryBinding(
      recoveryId,
      LifecycleManager.sourceRecoveryBindingId(sourceId),
      currentAnchor)
  }

  private def resolveRecoveryBinding(
      recoveryId: String,
      bindingId: String,
      currentValue: String): PbResolveSourceRecoveryAnchorResponse = {
    require(recoveryId != null && recoveryId.nonEmpty, "recoveryId must be non-empty")
    val lease = applicationLease
    if (lease == null) {
      throw new IllegalStateException("Cannot resolve a source anchor without an application lease")
    }
    masterClient.askSync[PbResolveSourceRecoveryAnchorResponse](
      PbResolveSourceRecoveryAnchor.newBuilder()
        .setAppId(appUniqueId)
        .setRecoveryId(recoveryId)
        .setSourceId(bindingId)
        .setCurrentAnchor(currentValue)
        .setApplicationLeaseEpoch(lease.epoch())
        .setApplicationLeaseOwnerId(lease.ownerId())
        .setRequestId(MasterClient.genRequestId())
        .build(),
      classOf[PbResolveSourceRecoveryAnchorResponse])
  }

  /**
   * Resolves the immutable ID of a resumable sink write. Write identities share the replicated
   * recovery-binding store with source anchors, but use disjoint length-delimited namespaces so a
   * sink can never alias a source identity. Keeping one CAS primitive gives both identities equal
   * lease fencing, snapshotting, and HA replay semantics.
   */
  def resolveWriteRecoveryId(
      recoveryId: String,
      sinkId: String,
      currentWriteId: String): PbResolveSourceRecoveryAnchorResponse = {
    require(sinkId != null && sinkId.nonEmpty, "sinkId must be non-empty")
    require(currentWriteId != null && currentWriteId.nonEmpty, "currentWriteId must be non-empty")
    resolveRecoveryBinding(
      recoveryId,
      LifecycleManager.writeRecoveryBindingId(sinkId),
      currentWriteId)
  }

  /**
   * Publishes a task envelope through the current driver lease and returns the canonical winner.
   */
  def publishRecoveryTaskCommit(
      recoveryId: String,
      writeId: String,
      partitionId: Int,
      payload: Array[Byte],
      sha256: Array[Byte]): PbPublishRecoveryTaskCommitResponse = {
    RecoveryTaskCommitUtils.validateIdentity(appUniqueId, recoveryId, writeId, partitionId)
    RecoveryTaskCommitUtils.validatePayload(
      payload, sha256, conf.recoveryTaskCommitMaxPayloadSize)
    val lease = requireRecoveryLease("publish a recovery task commit")
    masterClient.askSync[PbPublishRecoveryTaskCommitResponse](
      PbPublishRecoveryTaskCommit.newBuilder()
        .setAppId(appUniqueId)
        .setRecoveryId(recoveryId)
        .setWriteId(writeId)
        .setPartitionId(partitionId)
        .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
        .setSha256(com.google.protobuf.ByteString.copyFrom(sha256))
        .setApplicationLeaseEpoch(lease.epoch())
        .setApplicationLeaseOwnerId(lease.ownerId())
        .setRequestId(MasterClient.genRequestId())
        .build(),
      classOf[PbPublishRecoveryTaskCommitResponse])
  }

  /** Reads an authoritative task envelope through the current driver lease. */
  def getRecoveryTaskCommit(
      recoveryId: String,
      writeId: String,
      partitionId: Int): PbGetRecoveryTaskCommitResponse = {
    RecoveryTaskCommitUtils.validateIdentity(appUniqueId, recoveryId, writeId, partitionId)
    val lease = requireRecoveryLease("read a recovery task commit")
    masterClient.askSync[PbGetRecoveryTaskCommitResponse](
      PbGetRecoveryTaskCommit.newBuilder()
        .setAppId(appUniqueId)
        .setRecoveryId(recoveryId)
        .setWriteId(writeId)
        .setPartitionId(partitionId)
        .setApplicationLeaseEpoch(lease.epoch())
        .setApplicationLeaseOwnerId(lease.ownerId())
        .build(),
      classOf[PbGetRecoveryTaskCommitResponse])
  }

  def batchGetRecoveryTaskCommits(
      recoveryId: String,
      writeId: String,
      partitionIds: java.util.List[Integer]): PbBatchGetRecoveryTaskCommitsResponse = {
    require(partitionIds != null && !partitionIds.isEmpty && partitionIds.size() <= 1024,
      "partitionIds size must be within [1, 1024]")
    RecoveryTaskCommitUtils.validateIdentity(appUniqueId, recoveryId, writeId, -1)
    partitionIds.asScala.foreach { partitionId =>
      require(partitionId != null, "partitionIds must not contain null")
      RecoveryTaskCommitUtils.validateIdentity(
        appUniqueId, recoveryId, writeId, partitionId.intValue())
    }
    val lease = requireRecoveryLease("read recovery task commits")
    masterClient.askSync[PbBatchGetRecoveryTaskCommitsResponse](
      PbBatchGetRecoveryTaskCommits.newBuilder()
        .setAppId(appUniqueId)
        .setRecoveryId(recoveryId)
        .setWriteId(writeId)
        .addAllPartitionIds(partitionIds)
        .setApplicationLeaseEpoch(lease.epoch())
        .setApplicationLeaseOwnerId(lease.ownerId())
        .build(),
      classOf[PbBatchGetRecoveryTaskCommitsResponse])
  }

  private def requireRecoveryLease(operation: String): ApplicationLease = {
    val lease = applicationLease
    if (lease == null) {
      throw new IllegalStateException(s"Cannot $operation without an application lease")
    }
    lease
  }

  private[client] def publishCommittedShuffleCatalog(
      shuffleId: Int,
      catalog: PbCommittedShuffleCatalog): PbPublishCommittedShuffleCatalogResponse = {
    val lease = applicationLease
    if (lease == null) {
      throw new IllegalStateException(
        s"Cannot publish committed catalog for shuffle $shuffleId without an application lease")
    }
    masterClient.askSync[PbPublishCommittedShuffleCatalogResponse](
      PbPublishCommittedShuffleCatalog.newBuilder()
        .setAppId(appUniqueId)
        .setShuffleId(shuffleId)
        .setCatalog(catalog.toByteString)
        .setApplicationLeaseEpoch(lease.epoch())
        .setApplicationLeaseOwnerId(lease.ownerId())
        .setRequestId(MasterClient.genRequestId())
        .build(),
      classOf[PbPublishCommittedShuffleCatalogResponse])
  }

  def getCommittedShuffleCatalog(shuffleId: Int): PbGetCommittedShuffleCatalogResponse = {
    getCommittedShuffleCatalog(shuffleId, "")
  }

  def getCommittedShuffleCatalog(recoveryKey: String): PbGetCommittedShuffleCatalogResponse = {
    require(recoveryKey != null && recoveryKey.nonEmpty, "recoveryKey must be non-empty")
    getCommittedShuffleCatalog(0, recoveryKey)
  }

  private def getCommittedShuffleCatalog(
      shuffleId: Int,
      recoveryKey: String): PbGetCommittedShuffleCatalogResponse = {
    val lease = applicationLease
    if (lease == null) {
      throw new IllegalStateException(
        "Cannot fetch a committed catalog without an application lease")
    }
    masterClient.askSync[PbGetCommittedShuffleCatalogResponse](
      PbGetCommittedShuffleCatalog.newBuilder()
        .setAppId(appUniqueId)
        .setShuffleId(shuffleId)
        .setRecoveryKey(recoveryKey)
        .setApplicationLeaseEpoch(lease.epoch())
        .setApplicationLeaseOwnerId(lease.ownerId())
        .build(),
      classOf[PbGetCommittedShuffleCatalogResponse])
  }

  private def requestApplicationLease(
      expectedEpoch: Long,
      ownerId: String,
      expiresAtMs: Long,
      renewal: Boolean): PbApplicationLeaseControlResponse = {
    val response = masterClient.askSync[PbApplicationLeaseControlResponse](
      PbApplicationLeaseControl.newBuilder()
        .setAppId(appUniqueId)
        .setExpectedEpoch(expectedEpoch)
        .setOwnerId(ownerId)
        .setExpiresAtMs(expiresAtMs)
        .setRenewal(renewal)
        .setRequestId(MasterClient.genRequestId())
        .build(),
      classOf[PbApplicationLeaseControlResponse])
    if (response.getSuccess) {
      applicationLease =
        new ApplicationLease(response.getEpoch, response.getOwnerId, response.getExpiresAtMs)
    }
    response
  }

  // Since method `onStart` is executed when `rpcEnv.setupEndpoint` is executed, and
  // `masterClient` is initialized after `rpcEnv` is initialized, if method `onStart` contains
  // a reference to `masterClient`, there may be cases where `masterClient` is null when
  // `masterClient` is called. Therefore, it's necessary to uniformly execute the initialization
  // method at the end of the construction of the class to perform the initialization operations.
  private def initialize(): Unit = {
    registerApplicationInfo()
    // noinspection ConvertExpressionToSAM
    commitManager.start()
    heartbeater.start()
    changePartitionManager.start()
    releasePartitionManager.start()
  }

  override def onStart(): Unit = {
    // noinspection ConvertExpressionToSAM
    checkForShuffleRemoval = forwardMessageThread.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          self.send(RemoveExpiredShuffle)
        }
      },
      shuffleExpiredCheckIntervalMs,
      shuffleExpiredCheckIntervalMs,
      TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    checkForShuffleRemoval.cancel(true)
    ThreadUtils.shutdown(forwardMessageThread)

    commitManager.stop()
    changePartitionManager.stop()
    releasePartitionManager.stop()
    heartbeater.stop()

    masterClient.close()
    if (rpcEnv != null) {
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()
    }
    if (authEnabled) {
      if (masterRpcEnvInUse != null) {
        masterRpcEnvInUse.shutdown()
        masterRpcEnvInUse.awaitTermination()
      }
      if (workerRpcEnvInUse != null) {
        workerRpcEnvInUse.shutdown()
        workerRpcEnvInUse.awaitTermination()
      }
    }
    messagesHelper.close()
  }

  /**
   * Creates security context for external RPC endpoint.
   */
  def createRpcSecurityContext(
      appSecret: String,
      addClientRegistrationBootstrap: Boolean = false,
      registrationInfo: Option[RegistrationInfo] = None): Option[RpcSecurityContext] = {
    val clientSaslContextBuilder = new ClientSaslContextBuilder()
      .withAddRegistrationBootstrap(addClientRegistrationBootstrap)
      .withAppId(appUniqueId)
      .withSaslUser(appUniqueId)
      .withSaslPassword(appSecret)
    if (registrationInfo.isDefined) {
      clientSaslContextBuilder.withRegistrationInfo(registrationInfo.get)
    }
    val rpcSecurityContext = new RpcSecurityContextBuilder()
      .withClientSaslContext(clientSaslContextBuilder.build()).build()
    Some(rpcSecurityContext)
  }

  def getUserIdentifier: UserIdentifier = {
    userIdentifier
  }

  def getHost: String = {
    lifecycleHost
  }

  def getPort: Int = {
    rpcEnv.address.port
  }

  private val partitionType = conf.shufflePartitionType

  def getPartitionType(shuffleId: Int): PartitionType = {
    shufflePartitionType.getOrDefault(shuffleId, partitionType)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RemoveExpiredShuffle =>
      removeExpiredShuffle()
    case StageEnd(shuffleId) =>
      logInfo(s"Received StageEnd request, shuffleId $shuffleId.")
      handleStageEnd(shuffleId)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterShuffle(shuffleId, numMappers, numPartitions, serdeVersion) =>
      logDebug(s"Received RegisterShuffle request, " +
        s"$shuffleId, $numMappers, $numPartitions.")
      offerAndReserveSlots(
        RegisterCallContext(context),
        serdeVersion,
        shuffleId,
        numMappers,
        numPartitions)

    case pb: PbRegisterMapPartitionTask =>
      val shuffleId = pb.getShuffleId
      val numMappers = pb.getNumMappers
      val mapId = pb.getMapId
      val attemptId = pb.getAttemptId
      val partitionId = pb.getPartitionId
      val isSegmentGranularityVisible = pb.getIsSegmentGranularityVisible
      logDebug(s"Received Register map partition task request, " +
        s"$shuffleId, $numMappers, $mapId, $attemptId, $partitionId, $isSegmentGranularityVisible.")
      shufflePartitionType.putIfAbsent(shuffleId, PartitionType.MAP)
      offerAndReserveSlots(
        RegisterCallContext(context, partitionId),
        // Use V1 as this is only supported in java
        SerdeVersion.V1,
        shuffleId,
        numMappers,
        numMappers,
        partitionId,
        isSegmentGranularityVisible)

    case Revive(shuffleId, mapIds, reviveRequests, serdeVersion) =>
      val partitionIds = new util.ArrayList[Integer]()
      val epochs = new util.ArrayList[Integer]()
      val oldPartitions = new util.ArrayList[PartitionLocation]()
      val causes = new util.ArrayList[StatusCode]()
      (0 until reviveRequests.size()).foreach { idx =>
        val request = reviveRequests.get(idx)
        partitionIds.add(request.partitionId)
        epochs.add(request.epoch)
        oldPartitions.add(request.loc)
        causes.add(request.cause)
      }
      logDebug(s"Received Revive request, number of partitions ${partitionIds.size()}")
      handleRevive(
        context,
        shuffleId,
        mapIds,
        partitionIds,
        epochs,
        oldPartitions,
        causes,
        serdeVersion)

    case pb: PbPartitionSplit =>
      val shuffleId = pb.getShuffleId
      val partitionId = pb.getPartitionId
      val epoch = pb.getEpoch
      val oldPartition = PbSerDeUtils.fromPbPartitionLocation(pb.getOldPartition)
      logTrace(s"Received split request, " +
        s"$shuffleId, $partitionId, $epoch, $oldPartition")
      changePartitionManager.handleRequestPartitionLocation(
        // TODO: this message is not supported in cppClient yet.
        ChangeLocationsCallContext(context, 1, SerdeVersion.V1),
        shuffleId,
        partitionId,
        epoch,
        oldPartition,
        isSegmentGranularityVisible = commitManager.isSegmentGranularityVisible(shuffleId))

    case MapperEnd(
          shuffleId,
          mapId,
          attemptId,
          numMappers,
          partitionId,
          pushFailedBatch,
          numPartitions,
          crc32PerPartition,
          bytesWrittenPerPartition,
          serdeVersion) =>
      logTrace(s"Received MapperEnd TaskEnd request, " +
        s"${Utils.makeMapKey(shuffleId, mapId, attemptId)}")
      val partitionType = getPartitionType(shuffleId)
      partitionType match {
        case PartitionType.REDUCE =>
          handleMapperEnd(
            context,
            shuffleId,
            mapId,
            attemptId,
            numMappers,
            pushFailedBatch,
            numPartitions,
            crc32PerPartition,
            bytesWrittenPerPartition,
            serdeVersion)
        case PartitionType.MAP =>
          handleMapPartitionEnd(
            context,
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            numMappers,
            numPartitions,
            crc32PerPartition,
            bytesWrittenPerPartition,
            serdeVersion)
        case _ =>
          throw new UnsupportedOperationException(s"Not support $partitionType yet")
      }

    case pb: ReadReducerPartitionEnd =>
      val partitionType = getPartitionType(pb.shuffleId)
      partitionType match {
        // Map partitions reuse this reducer-named RPC/handler; for MAP, partitionId is the map
        // partition id and [startMapIndex, endMapIndex] is the consumed subpartition range.
        case PartitionType.REDUCE | PartitionType.MAP =>
          handleReducerPartitionEnd(
            context,
            pb.shuffleId,
            pb.partitionId,
            pb.startMapIndex,
            pb.endMapIndex,
            pb.crc32,
            pb.bytesWritten)
        case _ =>
          throw new UnsupportedOperationException(s"Not support $partitionType yet")
      }

    case GetReducerFileGroup(
          shuffleId: Int,
          isSegmentGranularityVisible: Boolean,
          serdeVersion: SerdeVersion) =>
      logDebug(
        s"Received GetShuffleFileGroup request for shuffleId $shuffleId, isSegmentGranularityVisible $isSegmentGranularityVisible")
      handleGetReducerFileGroup(context, shuffleId, isSegmentGranularityVisible, serdeVersion)

    case pb: PbGetStageEnd =>
      val shuffleId = pb.getShuffleId
      logDebug(s"Received GetStageEnd request for shuffleId $shuffleId")
      handleGetStageEnd(context, shuffleId)

    case pb: PbGetShuffleId =>
      val appShuffleId = pb.getAppShuffleId
      val appShuffleIdentifier = pb.getAppShuffleIdentifier
      val isWriter = pb.getIsShuffleWriter
      val isBarrierStage = pb.getIsBarrierStage
      logDebug(s"Received GetShuffleId request, appShuffleId $appShuffleId " +
        s"appShuffleIdentifier $appShuffleIdentifier isWriter $isWriter isBarrier $isBarrierStage.")
      handleGetShuffleIdForApp(
        context,
        appShuffleId,
        appShuffleIdentifier,
        isWriter,
        isBarrierStage)

    case pb: PbReportShuffleFetchFailure =>
      val appShuffleId = pb.getAppShuffleId
      val shuffleId = pb.getShuffleId
      val taskId = pb.getTaskId
      logDebug(s"Received ReportShuffleFetchFailure request, appShuffleId $appShuffleId shuffleId $shuffleId taskId $taskId")
      handleReportShuffleFetchFailure(context, appShuffleId, shuffleId, taskId)

    case pb: PbReportBarrierStageAttemptFailure =>
      val appShuffleId = pb.getAppShuffleId
      val appShuffleIdentifier = pb.getAppShuffleIdentifier
      logDebug(s"Received ReportBarrierStageAttemptFailure request, appShuffleId $appShuffleId, " +
        s"appShuffleIdentifier $appShuffleIdentifier")
      handleReportBarrierStageAttemptFailure(context, appShuffleId, appShuffleIdentifier)

    case pb: PbApplicationMetaRequest =>
      logDebug(s"Received request for meta info ${pb.getAppId}.")
      if (applicationMeta == null) {
        context.sendFailure(
          new IllegalArgumentException("Application meta is not initialized for this app."))
      } else {
        context.reply(PbSerDeUtils.toPbApplicationMeta(applicationMeta))
      }

    // Executors address the driver's LifecycleManager. Never forward executor-supplied lease
    // fields: this endpoint injects its own current lease, so a stale driver endpoint either no
    // longer exists or is rejected by the master after lease takeover.
    case pb: PbPublishRecoveryTaskCommit =>
      if (pb.getAppId != appUniqueId) {
        context.sendFailure(
          new IllegalArgumentException("Recovery task commit application mismatch"))
      } else {
        context.reply(publishRecoveryTaskCommit(
          pb.getRecoveryId,
          pb.getWriteId,
          pb.getPartitionId,
          pb.getPayload.toByteArray,
          pb.getSha256.toByteArray))
      }

    case pb: PbGetRecoveryTaskCommit =>
      if (pb.getAppId != appUniqueId) {
        context.sendFailure(
          new IllegalArgumentException("Recovery task commit application mismatch"))
      } else {
        context.reply(getRecoveryTaskCommit(
          pb.getRecoveryId, pb.getWriteId, pb.getPartitionId))
      }

    case pb: PbBatchGetRecoveryTaskCommits =>
      if (pb.getAppId != appUniqueId) {
        context.sendFailure(
          new IllegalArgumentException("Recovery task commit application mismatch"))
      } else {
        context.reply(batchGetRecoveryTaskCommits(
          pb.getRecoveryId, pb.getWriteId, pb.getPartitionIdsList))
      }
  }

  private def handleReducerPartitionEnd(
      context: RpcCallContext,
      shuffleId: Int,
      partitionId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      crc32: Int,
      bytesWritten: Long): Unit = {
    val (isValid, errorMessage) = commitManager.finishPartition(
      shuffleId,
      partitionId,
      startMapIndex,
      endMapIndex,
      new CommitMetadata(crc32, bytesWritten))
    var response: PbReadReducerPartitionEndResponse = null
    if (isValid) {
      response =
        PbReadReducerPartitionEndResponse.newBuilder().setStatus(
          StatusCode.SUCCESS.getValue).build()
    } else {
      response = PbReadReducerPartitionEndResponse.newBuilder().setStatus(
        StatusCode.READ_REDUCER_PARTITION_END_FAILED.getValue).setErrorMsg(errorMessage).build()
    }
    context.reply(response)
  }

  def setupEndpoints(
      workers: util.Set[WorkerInfo],
      shuffleId: Int,
      connectFailedWorkers: ShuffleFailedWorkers): Unit = {
    val futures = new util.LinkedList[(Future[RpcEndpointRef], WorkerInfo)]()
    workers.asScala foreach { workerInfo =>
      val future = workerRpcEnvInUse.asyncSetupEndpointRefByAddr(RpcEndpointAddress(
        RpcAddress.apply(workerInfo.host, workerInfo.rpcPort),
        WORKER_EP))
      futures.add((future, workerInfo))
    }

    var timeout = rpcAskTimeoutMs
    val delta = 50
    while (timeout > 0 && !futures.isEmpty) {
      val iter = futures.iterator
      while (iter.hasNext) {
        val (future, workerInfo) = iter.next()
        if (future.isCompleted) {
          future.value.get match {
            case scala.util.Success(endpointRef) =>
              workerInfo.endpoint = endpointRef
            case scala.util.Failure(e) =>
              logError(
                s"Init rpc client failed for $shuffleId on $workerInfo during reserve slots.",
                e)
              connectFailedWorkers.put(
                workerInfo,
                (StatusCode.WORKER_UNRESPONSIVE, System.currentTimeMillis()))
          }
          iter.remove()
        }
      }

      if (!futures.isEmpty) {
        Thread.sleep(delta)
        timeout -= delta
      }
    }
    if (!futures.isEmpty) {
      val iter = futures.iterator()
      while (iter.hasNext) {
        val (_, workerInfo) = iter.next()
        logError(s"Init rpc client failed for $shuffleId on $workerInfo during reserve slots, reason: Timeout.")
        connectFailedWorkers.put(
          workerInfo,
          (StatusCode.WORKER_UNRESPONSIVE, System.currentTimeMillis()))
      }
    }
  }

  private def offerAndReserveSlots(
      context: RegisterCallContext,
      serdeVersion: SerdeVersion,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int,
      partitionId: Int = -1,
      isSegmentGranularityVisible: Boolean = false): Unit = {
    val partitionType = getPartitionType(shuffleId)
    registeringShuffleRequest.synchronized {
      if (registeringShuffleRequest.containsKey(shuffleId)) {
        // If same request already exists in the registering request list for the same shuffle,
        // just register and return.
        logDebug(s"[handleRegisterShuffle] request for shuffle $shuffleId exists, just register")
        registeringShuffleRequest.get(shuffleId).add(context)
        return
      } else {
        // If shuffle is registered, reply this shuffle's partition location and return.
        // Else add this request to registeringShuffleRequest.
        if (registeredShuffle.contains(shuffleId)) {
          val rpcContext: RpcCallContext = context.context
          partitionType match {
            case PartitionType.MAP =>
              processMapTaskReply(
                shuffleId,
                rpcContext,
                serdeVersion,
                partitionId,
                getLatestLocs(shuffleId, p => p.getId == partitionId))
            case PartitionType.REDUCE =>
              if (rpcContext.isInstanceOf[LocalNettyRpcCallContext]) {
                context.reply(RegisterShuffleResponse(
                  StatusCode.SUCCESS,
                  getLatestLocs(shuffleId, _ => true),
                  serdeVersion))
              } else {
                val cachedMsg = registerShuffleResponseRpcCache.get(
                  shuffleId,
                  new Callable[ByteBuffer]() {
                    override def call(): ByteBuffer = {
                      rpcContext.asInstanceOf[RemoteNettyRpcCallContext].nettyEnv.serialize(
                        RegisterShuffleResponse(
                          StatusCode.SUCCESS,
                          getLatestLocs(shuffleId, _ => true),
                          serdeVersion))
                    }
                  })
                rpcContext.asInstanceOf[RemoteNettyRpcCallContext].callback.onSuccess(cachedMsg)
              }
            case _ =>
              throw new UnsupportedOperationException(s"Not support $partitionType yet")
          }
          return
        }

        logInfo(s"New shuffle request, shuffleId $shuffleId, partitionType: $partitionType " +
          s"numMappers: $numMappers, numReducers: $numPartitions.")
        val set = new util.HashSet[RegisterCallContext]()
        set.add(context)
        registeringShuffleRequest.put(shuffleId, set)
      }
    }

    def getLatestLocs(
        shuffleId: Int,
        partitionLocationFilter: PartitionLocation => Boolean): Array[PartitionLocation] = {
      workerSnapshots(shuffleId)
        .values()
        .asScala
        .flatMap(
          _.getAllPrimaryLocationsWithMaxEpoch()
        ) // get the partition with latest epoch of each worker
        .foldLeft(Map.empty[Int, PartitionLocation]) { (partitionLocationMap, partitionLocation) =>
          partitionLocationMap.get(partitionLocation.getId) match {
            case Some(existing) if existing.getEpoch >= partitionLocation.getEpoch =>
              partitionLocationMap
            case _ => partitionLocationMap + (partitionLocation.getId -> partitionLocation)
          }
        } // get the partition with latest epoch of all the partitions
        .values
        .filter(partitionLocationFilter)
        .toArray
    }

    def processMapTaskReply(
        shuffleId: Int,
        context: RpcCallContext,
        serdeVersion: SerdeVersion,
        partitionId: Int,
        partitionLocations: Array[PartitionLocation]): Unit = {
      // if any partition location resource exist just reply
      if (partitionLocations.size > 0) {
        context.reply(RegisterShuffleResponse(StatusCode.SUCCESS, partitionLocations, serdeVersion))
      } else {
        // request new resource for this task
        changePartitionManager.handleRequestPartitionLocation(
          ApplyNewLocationCallContext(context, serdeVersion),
          shuffleId,
          partitionId,
          -1,
          null,
          isSegmentGranularityVisible = commitManager.isSegmentGranularityVisible(shuffleId))
      }
    }

    // Reply to all RegisterShuffle request for current shuffle id.
    def replyRegisterShuffle(response: RegisterShuffleResponse): Unit = {
      registeringShuffleRequest.synchronized {
        val serializedMsg: Option[ByteBuffer] = partitionType match {
          case PartitionType.REDUCE =>
            context.context match {
              case remoteContext: RemoteNettyRpcCallContext =>
                if (response.status == StatusCode.SUCCESS) {
                  Option(remoteContext.nettyEnv.serialize(
                    response))
                } else {
                  Option.empty
                }

              case _ => Option.empty
            }
          case _ => Option.empty
        }

        val locations = response.partitionLocations

        registeringShuffleRequest.asScala
          .get(shuffleId)
          .foreach(_.asScala.foreach(context => {
            partitionType match {
              case PartitionType.MAP =>
                if (response.status == StatusCode.SUCCESS) {
                  val partitionLocations = locations.filter(_.getId == context.partitionId)
                  processMapTaskReply(
                    shuffleId,
                    context.context,
                    serdeVersion,
                    context.partitionId,
                    partitionLocations)
                } else {
                  // when register not success, need reply origin response,
                  // otherwise will lost original exception message
                  context.reply(response)
                }
              case PartitionType.REDUCE =>
                if (context.context.isInstanceOf[
                    LocalNettyRpcCallContext] || response.status != StatusCode.SUCCESS) {
                  context.reply(response)
                } else {
                  registerShuffleResponseRpcCache.put(shuffleId, serializedMsg.get)
                  context.context.asInstanceOf[RemoteNettyRpcCallContext].callback.onSuccess(
                    serializedMsg.get)
                }
              case _ =>
                throw new UnsupportedOperationException(s"Not support $partitionType yet")
            }
          }))
        registeringShuffleRequest.remove(shuffleId)
      }
    }

    // First, request to get allocated slots from Primary
    val ids = new util.ArrayList[Integer](numPartitions)
    (0 until numPartitions).foreach(idx => ids.add(Integer.valueOf(idx)))
    val res = requestMasterRequestSlotsWithRetry(shuffleId, ids)

    res.status match {
      case StatusCode.REQUEST_FAILED =>
        logInfo(s"OfferSlots RPC request failed for $shuffleId!")
        replyRegisterShuffle(RegisterShuffleResponse(
          StatusCode.REQUEST_FAILED,
          Array.empty,
          serdeVersion))
        return
      case StatusCode.SLOT_NOT_AVAILABLE =>
        logInfo(s"OfferSlots for $shuffleId failed!")
        replyRegisterShuffle(RegisterShuffleResponse(
          StatusCode.SLOT_NOT_AVAILABLE,
          Array.empty,
          serdeVersion))
        return
      case StatusCode.SUCCESS =>
        logDebug(s"OfferSlots for $shuffleId Success!Slots Info: ${res.workerResource}")
      case StatusCode.WORKER_EXCLUDED =>
        logInfo(s"OfferSlots for $shuffleId failed due to all workers be excluded!")
        replyRegisterShuffle(RegisterShuffleResponse(
          StatusCode.WORKER_EXCLUDED,
          Array.empty,
          serdeVersion))
        return
      case _ => // won't happen
        throw new UnsupportedOperationException()
    }

    // Reserve slots for each PartitionLocation. When response status is SUCCESS, WorkerResource
    // won't be empty since primary will reply SlotNotAvailable status when reserved slots is empty.
    val slots = res.workerResource
    val candidatesWorkers = new util.HashSet(slots.keySet())
    val connectFailedWorkers = new ShuffleFailedWorkers()

    // Second, for each worker, try to initialize the endpoint.
    setupEndpoints(slots.keySet(), shuffleId, connectFailedWorkers)
    candidatesWorkers.removeAll(connectFailedWorkers.asScala.keys.toList.asJava)
    workerStatusTracker.recordWorkerFailure(connectFailedWorkers)
    // If newly allocated from primary and can setup endpoint success, LifecycleManager should remove worker from
    // the excluded worker list to improve the accuracy of the list.
    workerStatusTracker.removeFromExcludedWorkers(candidatesWorkers)

    // Third, for each slot, LifecycleManager should ask Worker to reserve the slot
    // and prepare the pushing data env.
    val reserveSlotsSuccess =
      reserveSlotsWithRetry(
        shuffleId,
        candidatesWorkers,
        slots,
        updateEpoch = false,
        isSegmentGranularityVisible)

    // If reserve slots failed, clear allocated resources, reply ReserveSlotFailed and return.
    if (!reserveSlotsSuccess) {
      logError(s"reserve buffer for $shuffleId failed, reply to all.")
      replyRegisterShuffle(RegisterShuffleResponse(
        StatusCode.RESERVE_SLOTS_FAILED,
        Array.empty,
        serdeVersion))
    } else {
      if (log.isDebugEnabled()) {
        logDebug(s"ReserveSlots for $shuffleId success with details:$slots!")
      }
      // Forth, register shuffle success, update status
      val allocatedWorkers =
        JavaUtils.newConcurrentHashMap[String, ShufflePartitionLocationInfo]()
      slots.asScala.foreach { case (workerInfo, (primaryLocations, replicaLocations)) =>
        val partitionLocationInfo = new ShufflePartitionLocationInfo(workerInfo)
        partitionLocationInfo.addPrimaryPartitions(primaryLocations)
        updateLatestPartitionLocations(shuffleId, primaryLocations)
        partitionLocationInfo.addReplicaPartitions(replicaLocations)
        allocatedWorkers.put(workerInfo.toUniqueId, partitionLocationInfo)
      }
      shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
      registeredShuffle.add(shuffleId)
      commitManager.registerShuffle(
        shuffleId,
        numMappers,
        isSegmentGranularityVisible,
        numPartitions)

      // Fifth, reply the allocated partition location to ShuffleClient.
      logInfo(s"Handle RegisterShuffle Success for $shuffleId.")
      val allPrimaryPartitionLocations = slots.asScala.flatMap(_._2._1.asScala).toArray
      replyRegisterShuffle(RegisterShuffleResponse(
        StatusCode.SUCCESS,
        allPrimaryPartitionLocations,
        serdeVersion))
    }
  }

  private def handleRevive(
      context: RpcCallContext,
      shuffleId: Int,
      mapIds: util.List[Integer],
      partitionIds: util.List[Integer],
      oldEpochs: util.List[Integer],
      oldPartitions: util.List[PartitionLocation],
      causes: util.List[StatusCode],
      serdeVersion: SerdeVersion): Unit = {
    val contextWrapper =
      ChangeLocationsCallContext(context, partitionIds.size(), serdeVersion)
    // If shuffle not registered, reply ShuffleNotRegistered and return
    if (!registeredShuffle.contains(shuffleId)) {
      logError(s"[handleRevive] shuffle $shuffleId not registered!")
      contextWrapper.reply(
        -1,
        StatusCode.SHUFFLE_UNREGISTERED,
        None,
        false)
      return
    }
    logDebug(
      s"[handleRevive] shuffle $shuffleId, $mapIds, $partitionIds, $oldEpochs, $oldPartitions, $causes")
    if (commitManager.isStageEnd(shuffleId)) {
      logError(s"[handleRevive] shuffle $shuffleId stage ended!")
      contextWrapper.reply(
        -1,
        StatusCode.STAGE_ENDED,
        None,
        false)
      return
    }

    mapIds.asScala.foreach { mapId =>
      if (commitManager.isMapperEnded(shuffleId, mapId)) {
        logWarning(s"[handleRevive] Mapper ended, mapId $mapId, ended attemptId ${commitManager.getMapperAttempts(
          shuffleId)(mapId)}, shuffleId $shuffleId")
        contextWrapper.markMapperEnd(mapId)
      }
    }

    (0 until partitionIds.size()).foreach { idx =>
      changePartitionManager.handleRequestPartitionLocation(
        contextWrapper,
        shuffleId,
        partitionIds.get(idx),
        oldEpochs.get(idx),
        oldPartitions.get(idx),
        Some(causes.get(idx)),
        commitManager.isSegmentGranularityVisible(shuffleId))
    }
  }

  private def handleMapperEnd(
      context: RpcCallContext,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      pushFailedBatches: util.Map[String, LocationPushFailedBatches],
      numPartitions: Int,
      crc32PerPartition: Array[Int],
      bytesWrittenPerPartition: Array[Long],
      serdeVersion: SerdeVersion): Unit = {

    val (mapperAttemptFinishedSuccess, allMapperFinished) =
      commitManager.finishMapperAttempt(
        shuffleId,
        mapId,
        attemptId,
        numMappers,
        pushFailedBatches = pushFailedBatches,
        numPartitions = numPartitions,
        crc32PerPartition = crc32PerPartition,
        bytesWrittenPerPartition = bytesWrittenPerPartition)
    if (mapperAttemptFinishedSuccess && allMapperFinished) {
      // last mapper finished. call mapper end
      logInfo(s"Last MapperEnd, call StageEnd with shuffleKey:" +
        s"shuffleId $shuffleId.")
      self.send(StageEnd(shuffleId))
    }

    // reply success
    context.reply(MapperEndResponse(StatusCode.SUCCESS, serdeVersion))
  }

  private def handleGetReducerFileGroup(
      context: RpcCallContext,
      shuffleId: Int,
      isSegmentGranularityVisible: Boolean,
      serdeVersion: SerdeVersion): Unit = {
    // If isSegmentGranularityVisible is set to true, the downstream reduce task may start early than upstream map task, e.g. flink hybrid shuffle.
    // Under these circumstances, there's a possibility that the shuffle might not yet be registered when the downstream reduce task send GetReduceFileGroup request,
    // so we shouldn't send a SHUFFLE_NOT_REGISTERED response directly, should enqueue this request to pending list, and response to the downstream reduce task the ReduceFileGroup when the upstream map task register shuffle done
    if (!registeredShuffle.contains(shuffleId) && !isSegmentGranularityVisible) {
      logWarning(s"[handleGetReducerFileGroup] shuffle $shuffleId not registered, maybe no shuffle data within this stage.")
      context.reply(GetReducerFileGroupResponse(
        StatusCode.SHUFFLE_UNREGISTERED,
        JavaUtils.newConcurrentHashMap(),
        Array.empty,
        serdeVersion = serdeVersion))
      return
    }
    commitManager.handleGetReducerFileGroup(context, shuffleId, serdeVersion)
  }

  private def handleGetStageEnd(context: RpcCallContext, shuffleId: Int): Unit = {
    commitManager.handleGetStageEnd(context, shuffleId)
  }

  private def handleGetShuffleIdForApp(
      context: RpcCallContext,
      appShuffleId: Int,
      appShuffleIdentifier: String,
      isWriter: Boolean,
      isBarrierStage: Boolean): Unit = {
    val shuffleIds =
      if (isWriter) {
        shuffleIdMapping.computeIfAbsent(
          appShuffleId,
          new function.Function[
            Int,
            scala.collection.mutable.LinkedHashMap[String, (Int, Boolean)]]() {
            override def apply(id: Int)
                : scala.collection.mutable.LinkedHashMap[String, (Int, Boolean)] = {
              val newShuffleId = shuffleIdGenerator.getAndIncrement()
              logInfo(s"generate new shuffleId $newShuffleId for appShuffleId $appShuffleId appShuffleIdentifier $appShuffleIdentifier")
              celebornShuffleIdToAppShuffleIdMap.put(newShuffleId, appShuffleId)
              scala.collection.mutable.LinkedHashMap(appShuffleIdentifier -> (newShuffleId, true))
            }
          })
      } else {
        shuffleIdMapping.get(appShuffleId)
      }

    if (shuffleIds == null) {
      logWarning(s"unknown appShuffleId $appShuffleId, maybe no shuffle data for this shuffle")
      val pbGetShuffleIdResponse =
        PbGetShuffleIdResponse.newBuilder().setShuffleId(UNKNOWN_APP_SHUFFLE_ID).setSuccess(
          true).build()
      context.reply(pbGetShuffleIdResponse)
      return
    }

    def areAllMapTasksEnd(shuffleId: Int): Boolean = {
      commitManager.areAllMapperAttemptsFinished(shuffleId)
    }

    shuffleIds.synchronized {
      if (isWriter) {
        shuffleIds.get(appShuffleIdentifier) match {
          case Some((shuffleId, _)) =>
            val pbGetShuffleIdResponse =
              PbGetShuffleIdResponse.newBuilder().setShuffleId(shuffleId).setSuccess(true).build()
            context.reply(pbGetShuffleIdResponse)
          case None =>
            Option(appShuffleDeterminateMap.get(appShuffleId)).map { determinate =>
              val candidateShuffle =
                // For barrier stages, all tasks are re-executed when it is re-run : similar to indeterminate stage.
                // So if a barrier stage is getting reexecuted, previous stage/attempt needs to
                // be cleaned up as it is entirely unusuable
                if (determinate && !isBarrierStage && !isCelebornSkewShuffleOrChildShuffle(
                    appShuffleId))
                  shuffleIds.values.toSeq.reverse.find(e => e._2 == true)
                else
                  None

              val shuffleId: Integer =
                if (determinate && candidateShuffle.isDefined) {
                  val id = candidateShuffle.get._1
                  logInfo(s"reuse existing shuffleId $id for appShuffleId $appShuffleId appShuffleIdentifier $appShuffleIdentifier")
                  id
                } else {
                  // this branch means it is a redo of previous write stage
                  if (isBarrierStage) {
                    // unregister previous shuffle(s) which are still valid
                    val mapUpdates = shuffleIds.filter(_._2._2).map { kv =>
                      unregisterShuffle(kv._2._1)
                      kv._1 -> (kv._2._1, false)
                    }
                    shuffleIds ++= mapUpdates
                  }
                  val newShuffleId = shuffleIdGenerator.getAndIncrement()
                  logInfo(s"generate new shuffleId $newShuffleId for appShuffleId $appShuffleId appShuffleIdentifier $appShuffleIdentifier")
                  celebornShuffleIdToAppShuffleIdMap.put(newShuffleId, appShuffleId)
                  validateCelebornShuffleIdForClean.foreach(callback =>
                    callback.accept(appShuffleIdentifier))
                  shuffleIds.put(appShuffleIdentifier, (newShuffleId, true))
                  newShuffleId
                }
              val pbGetShuffleIdResponse =
                PbGetShuffleIdResponse.newBuilder().setShuffleId(shuffleId).setSuccess(true).build()
              context.reply(pbGetShuffleIdResponse)
            }.orElse(
              throw new UnsupportedOperationException(
                s"unexpected! unknown appShuffleId $appShuffleId when checking shuffle deterministic level"))
        }
      } else {
        shuffleIds.values.filter(v => v._2).map(v => v._1).toSeq.reverse.find(
          areAllMapTasksEnd) match {
          case Some(celebornShuffleId) =>
            val pbGetShuffleIdResponse = {
              logDebug(
                s"get shuffleId $celebornShuffleId for appShuffleId $appShuffleId appShuffleIdentifier $appShuffleIdentifier isWriter $isWriter")
              PbGetShuffleIdResponse.newBuilder().setShuffleId(celebornShuffleId).setSuccess(
                true).build()
            }
            context.reply(pbGetShuffleIdResponse)
          case None =>
            val pbGetShuffleIdResponse = {
              logInfo(
                s"there is no finished map stage associated with appShuffleId $appShuffleId")
              PbGetShuffleIdResponse.newBuilder().setShuffleId(UNKNOWN_APP_SHUFFLE_ID).setSuccess(
                false).build()
            }
            context.reply(pbGetShuffleIdResponse)
        }
      }
    }
  }

  private def handleReportShuffleFetchFailure(
      context: RpcCallContext,
      appShuffleId: Int,
      shuffleId: Int,
      taskId: Long): Unit = {

    val shuffleIds = shuffleIdMapping.get(appShuffleId)
    if (shuffleIds == null) {
      throw new UnsupportedOperationException(s"unexpected! unknown appShuffleId $appShuffleId")
    }
    var ret = true
    shuffleIds.synchronized {
      shuffleIds.find(e => e._2._1 == shuffleId) match {
        case Some((appShuffleIdentifier, (shuffleId, true))) =>
          if (invokeReportTaskShuffleFetchFailurePreCheck(taskId)) {
            logInfo(s"handle fetch failure for appShuffleId $appShuffleId shuffleId $shuffleId")
            ret = invokeAppShuffleTrackerCallback(appShuffleId)
            shuffleIds.put(appShuffleIdentifier, (shuffleId, false))
          } else {
            logInfo(
              s"Ignoring fetch failure from appShuffleIdentifier $appShuffleIdentifier shuffleId $shuffleId taskId $taskId")
            ret = false
          }
        case Some((appShuffleIdentifier, (shuffleId, false))) =>
          logInfo(
            s"Ignoring fetch failure from appShuffleIdentifier $appShuffleIdentifier shuffleId $shuffleId, " +
              "fetch failure is already reported and handled by other reader")
        case None => throw new UnsupportedOperationException(
            s"unexpected! unknown shuffleId $shuffleId for appShuffleId $appShuffleId")
      }
    }

    val pbReportShuffleFetchFailureResponse =
      PbReportShuffleFetchFailureResponse.newBuilder().setSuccess(ret).build()
    context.reply(pbReportShuffleFetchFailureResponse)
  }

  private def handleReportBarrierStageAttemptFailure(
      context: RpcCallContext,
      appShuffleId: Int,
      appShuffleIdentifier: String): Unit = {

    val shuffleIds = shuffleIdMapping.get(appShuffleId)
    if (shuffleIds == null) {
      throw new UnsupportedOperationException(s"unexpected! unknown appShuffleId $appShuffleId")
    }
    var ret = true
    shuffleIds.synchronized {
      shuffleIds.get(appShuffleIdentifier) match {
        case Some((shuffleId, true)) =>
          ret = invokeAppShuffleTrackerCallback(appShuffleId)
          unregisterShuffle(shuffleId)
          shuffleIds.put(appShuffleIdentifier, (shuffleId, false))
        case Some((shuffleId, false)) =>
          // older entry, already handled
          logInfo(
            s"Ignoring failure from barrier task for appShuffleIdentifier $appShuffleIdentifier " +
              s"shuffleId $shuffleId for appShuffleId $appShuffleId, already handled")
        case None =>
          throw new UnsupportedOperationException(
            s"unexpected! unknown appShuffleId $appShuffleId for appShuffleIdentifier = $appShuffleIdentifier")
      }
    }
    val pbReportBarrierStageAttemptFailureResponse =
      PbReportBarrierStageAttemptFailureResponse.newBuilder().setSuccess(ret).build()
    context.reply(pbReportBarrierStageAttemptFailureResponse)
  }

  private def invokeAppShuffleTrackerCallback(appShuffleId: Int): Boolean = {
    appShuffleTrackerCallback match {
      case Some(callback) =>
        try {
          callback.accept(appShuffleId)
          true
        } catch {
          case t: Throwable =>
            logError("Failed to invoke appShuffleTrackerCallback.", t)
            false
        }
      case None =>
        throw new UnsupportedOperationException(
          "unexpected! appShuffleTrackerCallback is not registered")
    }
  }

  private def invokeReportTaskShuffleFetchFailurePreCheck(taskId: Long): Boolean = {
    reportTaskShuffleFetchFailurePreCheck match {
      case Some(preCheck) =>
        try {
          preCheck.apply(taskId)
        } catch {
          case t: Throwable =>
            logError(s"Error preChecking the shuffle fetch failure reported by task: $taskId", t)
            false
        }
      case None => true
    }
  }

  private def isCelebornSkewShuffleOrChildShuffle(appShuffleId: Int): Boolean = {
    celebornSkewShuffleCheckCallback match {
      case Some(skewShuffleCallback) =>
        skewShuffleCallback.apply(appShuffleId)
      case None => false
    }
  }

  private def handleStageEnd(shuffleId: Int): Unit = {
    // check whether shuffle has registered
    if (!registeredShuffle.contains(shuffleId)) {
      logInfo(s"[handleStageEnd]" +
        s"$shuffleId not registered, maybe no shuffle data within this stage.")
      // record in stageEndShuffleSet
      commitManager.setStageEnd(shuffleId)
      return
    }

    if (commitManager.tryFinalCommit(shuffleId)) {
      // Here we only clear PartitionLocation info in shuffleAllocatedWorkers.
      // Since rerun or speculation task may running after we handle StageEnd.
      workerSnapshots(shuffleId).asScala.foreach { case (_, partitionLocationInfo) =>
        partitionLocationInfo.removeAllPrimaryPartitions()
        partitionLocationInfo.removeAllReplicaPartitions()
      }
    }
  }

  private def handleMapPartitionEnd(
      context: RpcCallContext,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      numMappers: Int,
      numPartitions: Int,
      crc32PerPartition: Array[Int],
      bytesWrittenPerPartition: Array[Long],
      serdeVersion: SerdeVersion): Unit = {
    def reply(result: Boolean): Unit = {
      val message =
        s"to handle MapPartitionEnd for ${Utils.makeMapKey(shuffleId, mapId, attemptId)}, " +
          s"$partitionId.";
      result match {
        case true => // if already committed by another try
          logDebug(s"Succeed $message")
          context.reply(MapperEndResponse(StatusCode.SUCCESS, serdeVersion))
        case false =>
          logError(s"Failed $message, reply ${StatusCode.SHUFFLE_DATA_LOST}.")
          context.reply(MapperEndResponse(StatusCode.SHUFFLE_DATA_LOST, serdeVersion))
      }
    }

    val (mapperAttemptFinishedSuccess, _) =
      commitManager.finishMapperAttempt(
        shuffleId,
        mapId,
        attemptId,
        numMappers,
        partitionId,
        numPartitions = numPartitions,
        crc32PerPartition = crc32PerPartition,
        bytesWrittenPerPartition = bytesWrittenPerPartition)
    reply(mapperAttemptFinishedSuccess)
  }

  def unregisterShuffle(shuffleId: Int): Unit = {
    if (getPartitionType(shuffleId) == PartitionType.REDUCE) {
      // if StageEnd has not been handled, trigger StageEnd
      if (!commitManager.isStageEnd(shuffleId)) {
        logInfo(s"Call StageEnd before Unregister Shuffle $shuffleId.")
        // try call stage end
        handleStageEnd(shuffleId)
        // wait stage end
        val (isTimeOut, cost) = commitManager.waitStageEnd(shuffleId)
        if (isTimeOut) {
          logError(s"[handleUnregisterShuffle] trigger StageEnd Timeout! $shuffleId.")
        } else {
          logInfo(s"[handleUnregisterShuffle] Wait for handleStageEnd complete costs ${cost}ms")
        }
      }
    }
    celebornShuffleIdToAppShuffleIdMap.remove(shuffleId)
    // add shuffleKey to delay shuffle removal set
    unregisterShuffleTime.put(shuffleId, System.currentTimeMillis())

    logInfo(s"Unregister for $shuffleId success.")
  }

  def unregisterAppShuffle(appShuffleId: Int, hasMapping: Boolean): Unit = {
    logInfo(s"Unregister appShuffleId $appShuffleId starts...")
    appShuffleDeterminateMap.remove(appShuffleId)
    if (hasMapping) {
      val shuffleIds = shuffleIdMapping.remove(appShuffleId)
      if (shuffleIds != null) {
        shuffleIds.synchronized(
          shuffleIds.values.map {
            case (shuffleId, _) =>
              unregisterShuffle(shuffleId)
              unregisterShuffleCallback.foreach(c => c.accept(shuffleId))
          })
      }
    } else {
      unregisterShuffle(appShuffleId)
    }
  }

  /* ========================================================== *
   |        END OF EVENT HANDLER                                |
   * ========================================================== */

  /**
   * After getting WorkerResource, LifecycleManger needs to ask each Worker to
   * reserve corresponding slot and prepare push data env in Worker side.
   *
   * @param shuffleId     Application shuffle id
   * @param slots         WorkerResource to reserve slots
   * @return List of reserving slot failed workers
   */
  private def reserveSlots(
      shuffleId: Int,
      slots: WorkerResource,
      isSegmentGranularityVisible: Boolean = false): util.List[WorkerInfo] = {
    val reserveSlotFailedWorkers = new ShuffleFailedWorkers()
    val failureInfos = new util.concurrent.CopyOnWriteArrayList[String]()
    val workerPartitionLocations = slots.asScala.filter(p => !p._2._1.isEmpty || !p._2._2.isEmpty)

    val (locsWithNullEndpoint, locs) = workerPartitionLocations.partition(_._1.endpoint == null)
    val futures = new LinkedBlockingQueue[(Future[ReserveSlotsResponse], WorkerInfo)]()
    val outFutures = locs.map { case (workerInfo, (primaryLocations, replicaLocations)) =>
      Future {
        val future = workerInfo.endpoint.ask[ReserveSlotsResponse](
          ReserveSlots(
            appUniqueId,
            shuffleId,
            primaryLocations,
            replicaLocations,
            partitionSplitThreshold,
            partitionSplitMode,
            getPartitionType(shuffleId),
            rangeReadFilter,
            userIdentifier,
            conf.pushDataTimeoutMs,
            partitionSplitEnabled = true,
            isSegmentGranularityVisible = isSegmentGranularityVisible))
        futures.add((future, workerInfo))
      }(ec)
    }
    val cbf =
      implicitly[
        CanBuildFrom[mutable.Iterable[Future[Boolean]], Boolean, mutable.Iterable[Boolean]]]
    val futureSeq = Future.sequence(outFutures)(cbf, ec)
    awaitResult(futureSeq, Duration.Inf)

    var timeout = rpcAskTimeoutMs
    val delta = 50
    while (timeout >= 0 && !futures.isEmpty) {
      val iter = futures.iterator()
      while (iter.hasNext) {
        val (future, workerInfo) = iter.next()
        if (future.isCompleted) {
          future.value.get match {
            case scala.util.Success(res) =>
              if (res.status.equals(StatusCode.SUCCESS)) {
                logDebug(s"Successfully allocated " +
                  s"partitions buffer for shuffleId $shuffleId" +
                  s" from worker ${workerInfo.readableAddress()}.")
              } else {
                failureInfos.add(s"[reserveSlots] Failed to" +
                  s" reserve buffers for shuffleId $shuffleId" +
                  s" from worker ${workerInfo.readableAddress()}. Reason: ${res.reason}")
                reserveSlotFailedWorkers.put(workerInfo, (res.status, System.currentTimeMillis()))
              }
            case scala.util.Failure(e) =>
              failureInfos.add(s"[reserveSlots] Failed to" +
                s" reserve buffers for shuffleId $shuffleId" +
                s" from worker ${workerInfo.readableAddress()}. Reason: $e")
              reserveSlotFailedWorkers.put(
                workerInfo,
                (StatusCode.REQUEST_FAILED, System.currentTimeMillis()))
          }
          iter.remove()
        }
      }

      if (!futures.isEmpty) {
        Thread.sleep(delta)
      }
      timeout = timeout - delta
    }

    val iter = futures.iterator()
    while (iter.hasNext) {
      val futureStatus = iter.next()
      val workerInfo = futureStatus._2
      failureInfos.add(s"[reserveSlots] Failed to" +
        s" reserve buffers for shuffleId $shuffleId" +
        s" from worker ${workerInfo.readableAddress()}. Reason: Timeout")
      reserveSlotFailedWorkers.put(
        workerInfo,
        (StatusCode.REQUEST_FAILED, System.currentTimeMillis()))
      iter.remove()
    }

    locsWithNullEndpoint.foreach { case (workerInfo, (_, _)) =>
      failureInfos.add(s"[reserveSlots] Failed to" +
        s" reserve buffers for shuffleId $shuffleId" +
        s" from worker ${workerInfo.readableAddress()}. Reason: null endpoint")
      reserveSlotFailedWorkers.put(
        workerInfo,
        (StatusCode.REQUEST_FAILED, System.currentTimeMillis()))
    }

    if (failureInfos.asScala.nonEmpty) {
      logError(s"Aggregated error of reserveSlots for " +
        s"shuffleId $shuffleId " +
        s"failure:${failureInfos.asScala.foldLeft("")((x, y) => s"$x \n $y")}")
    }
    workerStatusTracker.recordWorkerFailure(reserveSlotFailedWorkers)
    new util.ArrayList[WorkerInfo](reserveSlotFailedWorkers.asScala.keys.toList.asJava)
  }

  /**
   * When enabling replicate, if one of the partition location reserve slots failed,
   * LifecycleManager also needs to release another corresponding partition location.
   * To release the corresponding partition location, LifecycleManager should:
   *   1. Remove the peer partition location of failed partition location from slots.
   *   2. Request the Worker to destroy the slot's FileWriter.
   *   3. Request the Primary to release the worker slots status.
   *
   * @param shuffleId                shuffle id
   * @param slots                    allocated WorkerResource
   * @param failedPartitionLocations reserve slot failed partition location
   */
  private def releasePartitionLocation(
      shuffleId: Int,
      slots: WorkerResource,
      failedPartitionLocations: mutable.HashMap[Int, PartitionLocation],
      releasePeer: Boolean = false): Unit = {
    val destroyResource = new WorkerResource
    failedPartitionLocations.values
      .flatMap { partition => if (releasePeer) Option(partition.getPeer) else Option(partition) }
      .foreach { partition =>
        var destroyWorkerInfo = partition.getWorker
        val workerInfoWithRpcRef = slots.keySet().asScala.find(_.equals(destroyWorkerInfo))
          .getOrElse {
            logWarning(s"Cannot find workInfo for $shuffleId from previous success workResource:" +
              s" ${destroyWorkerInfo.readableAddress()}, init according to partition info")
            try {
              if (!workerStatusTracker.workerExcluded(destroyWorkerInfo)) {
                destroyWorkerInfo.endpoint = workerRpcEnvInUse.setupEndpointRef(
                  RpcAddress.apply(destroyWorkerInfo.host, destroyWorkerInfo.rpcPort),
                  WORKER_EP)
              } else {
                logInfo(
                  s"${destroyWorkerInfo.toUniqueId} is unavailable, set destroyWorkerInfo to null")
                destroyWorkerInfo = null
              }
            } catch {
              case t: Throwable =>
                logError(
                  s"Init rpc client failed for $shuffleId on ${destroyWorkerInfo.readableAddress()} during release peer partition.",
                  t)
                destroyWorkerInfo = null
            }
            destroyWorkerInfo
          }
        if (slots.containsKey(workerInfoWithRpcRef)) {
          val (primaryPartitionLocations, replicaPartitionLocations) =
            slots.get(workerInfoWithRpcRef)
          partition.getMode match {
            case PartitionLocation.Mode.PRIMARY =>
              primaryPartitionLocations.remove(partition)
              destroyResource.computeIfAbsent(workerInfoWithRpcRef, newLocationFunc)
                ._1.add(partition)
            case PartitionLocation.Mode.REPLICA =>
              replicaPartitionLocations.remove(partition)
              destroyResource.computeIfAbsent(workerInfoWithRpcRef, newLocationFunc)
                ._2.add(partition)
          }
          if (primaryPartitionLocations.isEmpty && replicaPartitionLocations.isEmpty) {
            slots.remove(workerInfoWithRpcRef)
          }
        }
      }
    if (!destroyResource.isEmpty) {
      destroySlotsWithRetry(shuffleId, destroyResource)
      val msg = destroyResource.asScala.map(entry =>
        s"${entry._1.endpoint}, ${entry._2._1.asScala.map(
          _.getUniqueId)}, ${entry._2._2.asScala.map(_.getUniqueId)}")
      logWarning(s"Destroyed partitions for reserve buffer failed workers " +
        s"shuffleId $shuffleId, $msg")
    }
  }

  /**
   * Collect all allocated partition locations on reserving slot failed workers
   * and remove failed worker's partition locations from total slots.
   * For each reduce id, we only need to maintain one of the pair locations
   * even if enabling replicate. If Celeborn wants to release the failed partition location,
   * the corresponding peers will be handled in [[releasePartitionLocation]]
   *
   * @param reserveFailedWorkers reserve slot failed WorkerInfo list of slots
   * @param slots                the slots tried to reserve a slot
   * @return reserving slot failed partition locations
   */
  def getFailedPartitionLocations(
      reserveFailedWorkers: util.List[WorkerInfo],
      slots: WorkerResource): mutable.HashMap[Int, PartitionLocation] = {
    val failedPartitionLocations = new mutable.HashMap[Int, PartitionLocation]()
    reserveFailedWorkers.asScala.foreach { workerInfo =>
      val (failedPrimaryLocations, failedReplicaLocations) = slots.get(workerInfo)
      if (null != failedPrimaryLocations) {
        failedPrimaryLocations.asScala.foreach { failedPrimaryLocation =>
          failedPartitionLocations += (failedPrimaryLocation.getId -> failedPrimaryLocation)
        }
      }
      if (null != failedReplicaLocations) {
        failedReplicaLocations.asScala.foreach { failedReplicaLocation =>
          val partitionId = failedReplicaLocation.getId
          if (!failedPartitionLocations.contains(partitionId)) {
            failedPartitionLocations += (partitionId -> failedReplicaLocation)
          }
        }
      }
    }
    failedPartitionLocations
  }

  /**
   * Reserve buffers with retry, retry on another node will cause slots to be inconsistent.
   *
   * @param shuffleId     shuffle id
   * @param candidates    working worker list
   * @param slots         the total allocated worker resources that need to be applied for the slot
   * @return If reserve all slots success
   */
  def reserveSlotsWithRetry(
      shuffleId: Int,
      candidates: util.HashSet[WorkerInfo],
      slots: WorkerResource,
      updateEpoch: Boolean = true,
      isSegmentGranularityVisible: Boolean = false): Boolean = {
    var requestSlots = slots
    val reserveSlotsMaxRetries = conf.clientReserveSlotsMaxRetries
    val reserveSlotsRetryWait = conf.clientReserveSlotsRetryWait
    var retryTimes = 1
    var noAvailableSlots = false
    var success = false
    while (retryTimes <= reserveSlotsMaxRetries && !success && !noAvailableSlots) {
      if (retryTimes > 1) {
        Thread.sleep(reserveSlotsRetryWait)
      }
      // reserve buffers
      logInfo(s"Try reserve slots for $shuffleId for $retryTimes times.")
      val reserveFailedWorkers = reserveSlots(shuffleId, requestSlots, isSegmentGranularityVisible)
      if (reserveFailedWorkers.isEmpty) {
        success = true
      } else {
        // Should remove failed workers from candidates during retry to avoid reallocate in failed workers.
        candidates.removeAll(reserveFailedWorkers)
        // Find out all failed partition locations and remove failed worker's partition location
        // from slots.
        val failedPartitionLocations =
          getFailedPartitionLocations(reserveFailedWorkers, slots)
        // When enable replicate, if one of the partition location reserve slots failed, we also
        // need to release another corresponding partition location and remove it from slots.
        if (failedPartitionLocations.nonEmpty && !slots.isEmpty) {
          releasePartitionLocation(shuffleId, slots, failedPartitionLocations)
        }
        if (pushReplicateEnabled && failedPartitionLocations.nonEmpty && !slots.isEmpty) {
          releasePartitionLocation(shuffleId, slots, failedPartitionLocations, true)
        }
        if (retryTimes < reserveSlotsMaxRetries) {
          // get retryCandidates resource and retry reserve buffer
          val retryCandidates = new util.HashSet(slots.keySet())
          // add candidates to avoid revive action passed in slots only 2 worker
          retryCandidates.addAll(candidates)
          // remove excluded workers from retryCandidates
          retryCandidates.removeAll(
            workerStatusTracker.excludedWorkers.keys().asScala.toList.asJava)
          retryCandidates.removeAll(workerStatusTracker.shuttingWorkers.asScala.toList.asJava)
          if (retryCandidates.size < 1 || (pushReplicateEnabled && retryCandidates.size < 2)) {
            logError(s"Retry reserve slots for $shuffleId failed caused by not enough slots.")
            noAvailableSlots = true
          } else {
            // Only when the LifecycleManager needs to retry reserve slots again, re-allocate slots
            // and put the new allocated slots to the total slots, the re-allocated slots won't be
            // duplicated with existing partition locations.
            requestSlots = reallocateSlotsFromCandidates(
              failedPartitionLocations.values.toList,
              retryCandidates.asScala.toList,
              updateEpoch)
            requestSlots.asScala.foreach {
              case (workerInfo, (retryPrimaryLocs, retryReplicaLocs)) =>
                val (primaryPartitionLocations, replicaPartitionLocations) =
                  slots.computeIfAbsent(workerInfo, newLocationFunc)
                primaryPartitionLocations.addAll(retryPrimaryLocs)
                replicaPartitionLocations.addAll(retryReplicaLocs)
            }
          }
        } else {
          logError(s"Try reserve slots for $shuffleId failed after $reserveSlotsMaxRetries retry.")
        }
      }
      retryTimes += 1
    }
    // if failed after retry, destroy all allocated buffers
    if (!success) {
      // Reserve slot failed workers' partition location and corresponding peer partition location
      // has been removed from slots by call [[getFailedPartitionLocations]] and
      // [[releasePartitionLocation]]. Now in the slots are all the successful partition
      // locations.
      logWarning(s"Reserve buffers for $shuffleId still fail after retrying, clear buffers.")
      destroySlotsWithRetry(shuffleId, slots)
    } else {
      logInfo(s"Reserve buffer success for shuffleId $shuffleId")
    }
    success
  }

  val newLocationFunc =
    new util.function.Function[WorkerInfo, (JList[PartitionLocation], JList[PartitionLocation])] {
      override def apply(w: WorkerInfo): (JList[PartitionLocation], JList[PartitionLocation]) =
        (new util.LinkedList[PartitionLocation](), new util.LinkedList[PartitionLocation]())
    }

  /**
   * Allocate a new primary/replica PartitionLocation pair from the current WorkerInfo list.
   *
   * @param oldEpochId Current partition reduce location last epoch id
   * @param candidates WorkerInfo list can be used to offer worker slots
   * @param slots      Current WorkerResource
   */
  def allocateFromCandidates(
      id: Int,
      oldEpochId: Int,
      candidates: List[WorkerInfo],
      slots: WorkerResource,
      updateEpoch: Boolean = true): Unit = {

    def isOnSameRack(primaryIndex: Int, replicaIndex: Int): Boolean = {
      candidates(primaryIndex).networkLocation.equals(candidates(replicaIndex).networkLocation)
    }

    val primaryIndex = Random.nextInt(candidates.size)
    val primaryLocation = new PartitionLocation(
      id,
      if (updateEpoch) oldEpochId + 1 else oldEpochId,
      candidates(primaryIndex).host,
      candidates(primaryIndex).rpcPort,
      candidates(primaryIndex).pushPort,
      candidates(primaryIndex).fetchPort,
      candidates(primaryIndex).replicatePort,
      PartitionLocation.Mode.PRIMARY,
      null,
      new StorageInfo("", storageTypes.head, availableStorageTypes),
      new RoaringBitmap())
    if (pushReplicateEnabled) {
      var replicaIndex = (primaryIndex + 1) % candidates.size
      while (pushRackAwareEnabled && isOnSameRack(primaryIndex, replicaIndex)
        && replicaIndex != primaryIndex) {
        replicaIndex = (replicaIndex + 1) % candidates.size
      }
      // If one turn no suitable peer, then just use the next worker.
      if (replicaIndex == primaryIndex) {
        replicaIndex = (primaryIndex + 1) % candidates.size
      }
      val replicaLocation = new PartitionLocation(
        id,
        if (updateEpoch) oldEpochId + 1 else oldEpochId,
        candidates(replicaIndex).host,
        candidates(replicaIndex).rpcPort,
        candidates(replicaIndex).pushPort,
        candidates(replicaIndex).fetchPort,
        candidates(replicaIndex).replicatePort,
        PartitionLocation.Mode.REPLICA,
        primaryLocation,
        new StorageInfo("", storageTypes.head, availableStorageTypes),
        new RoaringBitmap())
      primaryLocation.setPeer(replicaLocation)
      val primaryAndReplicaPairs = slots.computeIfAbsent(candidates(replicaIndex), newLocationFunc)
      primaryAndReplicaPairs._2.add(replicaLocation)
    }

    val primaryAndReplicaPairs = slots.computeIfAbsent(candidates(primaryIndex), newLocationFunc)
    primaryAndReplicaPairs._1.add(primaryLocation)
  }

  private def reallocateSlotsFromCandidates(
      oldPartitions: List[PartitionLocation],
      candidates: List[WorkerInfo],
      updateEpoch: Boolean = true): WorkerResource = {
    val slots = new WorkerResource()
    oldPartitions.foreach { partition =>
      allocateFromCandidates(partition.getId, partition.getEpoch, candidates, slots, updateEpoch)
    }
    slots
  }

  case class DestroyFutureWithStatus(
      var future: Future[DestroyWorkerSlotsResponse],
      message: DestroyWorkerSlots,
      endpoint: RpcEndpointRef,
      var retryTimes: Int,
      var startTime: Long)

  /**
   * For the slots that need to be destroyed, LifecycleManager will ask the corresponding worker
   * to destroy related FileWriter.
   *
   * @param shuffleId      shuffle id
   * @param slotsToDestroy worker resource to be destroyed
   * @return destroy failed primary and replica location unique id
   */
  def destroySlotsWithRetry(
      shuffleId: Int,
      slotsToDestroy: WorkerResource): Unit = {
    val shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId)

    def retryDestroy(status: DestroyFutureWithStatus, currentTime: Long): Unit = {
      status.retryTimes += 1
      status.startTime = currentTime
      // mock failure if mockDestroyFailure is true and this is not the last retry
      status.message.mockFailure =
        status.message.mockFailure && (status.retryTimes != rpcMaxRetires)
      status.future =
        status.endpoint.ask[DestroyWorkerSlotsResponse](status.message)
    }

    val startTime = System.currentTimeMillis()
    val futures = new util.LinkedList[DestroyFutureWithStatus]()
    slotsToDestroy.asScala.filter(_._1.endpoint != null).foreach {
      case (workerInfo, (primaryLocations, replicaLocations)) =>
        val primaryIds = primaryLocations.asScala.map(_.getUniqueId).asJava
        val replicaIds = replicaLocations.asScala.map(_.getUniqueId).asJava
        val lease = applicationLease
        val destroy = DestroyWorkerSlots(
          shuffleKey,
          primaryIds,
          replicaIds,
          mockDestroyFailure,
          if (lease == null) 0L else lease.epoch(),
          if (lease == null) "" else lease.ownerId())
        val future = workerInfo.endpoint.ask[DestroyWorkerSlotsResponse](destroy)
        futures.add(DestroyFutureWithStatus(future, destroy, workerInfo.endpoint, 1, startTime))
    }

    val timeout = rpcAskTimeoutMs
    var remainingTime = timeout * rpcMaxRetires
    val delta = 50
    while (remainingTime > 0 && !futures.isEmpty) {
      val currentTime = System.currentTimeMillis()
      val iter = futures.iterator()
      while (iter.hasNext) {
        val futureWithStatus = iter.next()
        val message = futureWithStatus.message
        val retryTimes = futureWithStatus.retryTimes
        if (futureWithStatus.future.isCompleted) {
          futureWithStatus.future.value.get match {
            case scala.util.Success(res) =>
              if (res.status != StatusCode.SUCCESS && retryTimes < rpcMaxRetires) {
                logError(
                  s"Request $message to ${futureWithStatus.endpoint} return ${res.status} for $shuffleKey $retryTimes/$rpcMaxRetires, " +
                    "will retry.")
                retryDestroy(futureWithStatus, currentTime)
              } else {
                if (res.status != StatusCode.SUCCESS && retryTimes == rpcMaxRetires) {
                  logError(
                    s"Request $message to ${futureWithStatus.endpoint} return ${res.status} for $shuffleKey $retryTimes/$rpcMaxRetires, " +
                      "will not retry.")
                }
                iter.remove()
              }
            case scala.util.Failure(e) =>
              if (retryTimes < rpcMaxRetires) {
                logError(
                  s"Request $message to ${futureWithStatus.endpoint} failed $retryTimes/$rpcMaxRetires for $shuffleKey, reason: $e, " +
                    "will retry.")
                retryDestroy(futureWithStatus, currentTime)
              } else {
                if (retryTimes == rpcMaxRetires) {
                  logError(
                    s"Request $message to ${futureWithStatus.endpoint} failed $retryTimes/$rpcMaxRetires for $shuffleKey, reason: $e, " +
                      "will not retry.")
                }
                iter.remove()
              }
          }
        } else if (currentTime - futureWithStatus.startTime > timeout) {
          if (retryTimes < rpcMaxRetires) {
            logError(
              s"Request $message to ${futureWithStatus.endpoint} failed $retryTimes/$rpcMaxRetires for $shuffleKey, reason: Timeout, " +
                "will retry.")
            retryDestroy(futureWithStatus, currentTime)
          } else {
            if (retryTimes == rpcMaxRetires) {
              logError(
                s"Request $message to ${futureWithStatus.endpoint} failed $retryTimes/$rpcMaxRetires for $shuffleKey, reason: Timeout, " +
                  "will retry.")
            }
            iter.remove()
          }
        }
      }

      if (!futures.isEmpty) {
        Thread.sleep(delta)
        remainingTime -= delta
      }
    }
    futures.clear()
  }

  private def removeExpiredShuffle(): Unit = {
    val currentTime = System.currentTimeMillis()
    val shuffleIdsToRemove = new ArrayBuffer[Integer]
    unregisterShuffleTime.keys().asScala.foreach { shuffleId =>
      if (unregisterShuffleTime.get(shuffleId) < currentTime - shuffleExpiredCheckIntervalMs) {
        shuffleIdsToRemove += shuffleId
        // clear for the shuffle
        registeredShuffle.remove(shuffleId)
        registeringShuffleRequest.remove(shuffleId)
        shuffleAllocatedWorkers.remove(shuffleId)
        latestPartitionLocation.remove(shuffleId)
        commitManager.removeExpiredShuffle(shuffleId)
        changePartitionManager.removeExpiredShuffle(shuffleId)
        invalidatedBroadcastGetReducerFileGroupResponse(shuffleId)
      }
    }

    if (shuffleIdsToRemove.nonEmpty) {
      logInfo(s"Clear shuffleIds: (${shuffleIdsToRemove.mkString(", ")}).")
      val lease = applicationLease
      val leaseEpoch = Option(lease).map(_.epoch()).getOrElse(0L)
      val leaseOwnerId = Option(lease).map(_.ownerId()).getOrElse("")
      if (!batchRemoveExpiredShufflesEnabled) {
        shuffleIdsToRemove.foreach { shuffleId =>
          val unregisterShuffleResponse = requestMasterUnregisterShuffle(
            UnregisterShuffle(
              appUniqueId,
              shuffleId,
              MasterClient.genRequestId(),
              leaseEpoch,
              leaseOwnerId))
          // if unregister shuffle not success, wait next turn
          if (StatusCode.SUCCESS == StatusCode.fromValue(unregisterShuffleResponse.getStatus)) {
            unregisterShuffleTime.remove(shuffleId)
          }
        }
      } else {
        val unregisterShuffleResponse = batchRequestMasterUnregisterShuffles(
          BatchUnregisterShuffles(
            appUniqueId,
            shuffleIdsToRemove.asJava,
            MasterClient.genRequestId(),
            leaseEpoch,
            leaseOwnerId))
        if (StatusCode.SUCCESS == StatusCode.fromValue(unregisterShuffleResponse.getStatus)) {
          shuffleIdsToRemove.foreach { shuffleId: Integer =>
            unregisterShuffleTime.remove(shuffleId)
          }
        }
      }
    }
  }

  def requestMasterRequestSlotsWithRetry(
      shuffleId: Int,
      ids: util.ArrayList[Integer]): RequestSlotsResponse = {
    val excludedWorkerSet =
      if (excludedWorkersFilter) {
        workerStatusTracker.excludedWorkers.asScala.keys.toSet
      } else {
        Set.empty[WorkerInfo]
      }
    // UserResourceConsumption and DiskInfo are eliminated from WorkerInfo
    // during serialization of RequestSlots
    val req =
      RequestSlots(
        appUniqueId,
        shuffleId,
        ids,
        lifecycleHost,
        pushReplicateEnabled,
        pushRackAwareEnabled,
        userIdentifier,
        slotsAssignMaxWorkers,
        availableStorageTypes,
        excludedWorkerSet,
        packed = true,
        tagsExpr = clientTagsExpr,
        applicationLeaseEpoch = Option(applicationLease).map(_.epoch()).getOrElse(0L),
        applicationLeaseOwnerId = Option(applicationLease).map(_.ownerId()).getOrElse(""))
    val res = requestMasterRequestSlots(req)
    if (res.status != StatusCode.SUCCESS) {
      requestMasterRequestSlots(req)
    } else {
      res
    }
  }

  private def requestMasterRequestSlots(message: RequestSlots): RequestSlotsResponse = {
    val shuffleKey = Utils.makeShuffleKey(message.applicationId, message.shuffleId)
    try {
      masterClient.askSync[RequestSlotsResponse](message, classOf[RequestSlotsResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync RegisterShuffle for $shuffleKey failed.", e)
        RequestSlotsResponse(StatusCode.REQUEST_FAILED, new WorkerResource(), message.packed)
    }
  }

  private def requestWorkerReserveSlots(
      endpoint: RpcEndpointRef,
      message: ReserveSlots): ReserveSlotsResponse = {
    val shuffleKey = Utils.makeShuffleKey(message.applicationId, message.shuffleId)
    try {
      endpoint.askSync[ReserveSlotsResponse](message, conf.clientRpcReserveSlotsRpcTimeout)
    } catch {
      case e: Exception =>
        val msg =
          s"Exception when askSync worker(${endpoint.address}) ReserveSlots for $shuffleKey " +
            s"on worker $endpoint."
        logError(msg, e)
        ReserveSlotsResponse(StatusCode.REQUEST_FAILED, msg + s" ${e.getMessage}")
    }
  }

  private def requestMasterUnregisterShuffle(message: PbUnregisterShuffle)
      : PbUnregisterShuffleResponse = {
    try {
      masterClient.askSync[PbUnregisterShuffleResponse](
        message,
        classOf[PbUnregisterShuffleResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync UnregisterShuffle for ${message.getShuffleId} failed.", e)
        UnregisterShuffleResponse(StatusCode.REQUEST_FAILED)
    }
  }

  private def batchRequestMasterUnregisterShuffles(message: PbBatchUnregisterShuffles)
      : PbBatchUnregisterShuffleResponse = {
    try {
      logInfo(s"AskSync BatchUnregisterShuffle for ${message.getShuffleIdsList}")
      masterClient.askSync[PbBatchUnregisterShuffleResponse](
        message,
        classOf[PbBatchUnregisterShuffleResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync BatchUnregisterShuffle for ${message.getShuffleIdsList} failed.", e)
        BatchUnregisterShuffleResponse(StatusCode.REQUEST_FAILED)
    }
  }

  def checkQuota(): CheckQuotaResponse = {
    try {
      masterClient.askSync[CheckQuotaResponse](
        CheckQuota(userIdentifier),
        classOf[CheckQuotaResponse])
    } catch {
      case e: Exception =>
        val msg = s"AskSync Cluster check quota for $userIdentifier failed."
        logError(msg, e)
        CheckQuotaResponse(false, msg)
    }
  }

  def checkWorkersAvailable(): PbCheckWorkersAvailableResponse = {
    try {
      masterClient.askSync[PbCheckWorkersAvailableResponse](
        CheckWorkersAvailable(),
        classOf[PbCheckWorkersAvailableResponse])
    } catch {
      case e: Exception =>
        val msg = s"AskSync Cluster check workers available for $userIdentifier failed."
        logError(msg, e)
        CheckWorkersAvailableResponse(false)
    }
  }

  // Once a partition is released, it will never be needed anymore
  def releasePartition(shuffleId: Int, partitionId: Int): Unit = {
    commitManager.releasePartitionResource(shuffleId, partitionId)
    val partitionLocation = latestPartitionLocation.get(shuffleId)
    if (partitionLocation != null) {
      partitionLocation.remove(partitionId)
    }

    releasePartitionManager.releasePartition(shuffleId, partitionId)
  }

  def getAllocatedWorkers(): Set[WorkerInfo] = {
    shuffleAllocatedWorkers.asScala.values.flatMap(_.values().asScala.map(_.workerInfo)).toSet
  }

  // delegate workerStatusTracker to register listener
  def registerWorkerStatusListener(workerStatusListener: WorkerStatusListener): Unit = {
    workerStatusTracker.registerWorkerStatusListener(workerStatusListener)
  }

  @volatile private var reportTaskShuffleFetchFailurePreCheck
      : Option[java.util.function.Function[java.lang.Long, Boolean]] = None
  def registerReportTaskShuffleFetchFailurePreCheck(preCheck: java.util.function.Function[
    java.lang.Long,
    Boolean]): Unit = {
    reportTaskShuffleFetchFailurePreCheck = Some(preCheck)
  }

  @volatile private var appShuffleTrackerCallback: Option[Consumer[Integer]] = None
  def registerShuffleTrackerCallback(callback: Consumer[Integer]): Unit = {
    appShuffleTrackerCallback = Some(callback)
  }

  // expecting celeborn shuffle id and application shuffle identifier
  @volatile private var validateCelebornShuffleIdForClean: Option[Consumer[String]] =
    None
  def registerValidateCelebornShuffleIdForCleanCallback(
      callback: Consumer[String]): Unit = {
    validateCelebornShuffleIdForClean = Some(callback)
  }

  @volatile private var unregisterShuffleCallback: Option[Consumer[Integer]] = None
  def registerUnregisterShuffleCallback(callback: Consumer[Integer]): Unit = {
    unregisterShuffleCallback = Some(callback)
  }

  def registerAppShuffleDeterminate(appShuffleId: Int, determinate: Boolean): Unit = {
    appShuffleDeterminateMap.put(appShuffleId, determinate)
  }

  @volatile private var cancelShuffleCallback: Option[BiConsumer[Integer, String]] = None
  def registerCancelShuffleCallback(callback: BiConsumer[Integer, String]): Unit = {
    cancelShuffleCallback = Some(callback)
  }

  @volatile private var broadcastGetReducerFileGroupResponseCallback
      : Option[java.util.function.BiFunction[Integer, GetReducerFileGroupResponse, Array[Byte]]] =
    None
  def registerBroadcastGetReducerFileGroupResponseCallback(call: java.util.function.BiFunction[
    Integer,
    GetReducerFileGroupResponse,
    Array[Byte]]): Unit = {
    broadcastGetReducerFileGroupResponseCallback = Some(call)
  }

  @volatile private var invalidatedBroadcastCallback: Option[Consumer[Integer]] =
    None
  def registerInvalidatedBroadcastCallback(call: Consumer[Integer]): Unit = {
    invalidatedBroadcastCallback = Some(call)
  }

  def invalidateLatestMaxLocsCache(shuffleId: Int): Unit = {
    registerShuffleResponseRpcCache.invalidate(shuffleId)
  }

  @volatile private var celebornSkewShuffleCheckCallback
      : Option[function.Function[Integer, Boolean]] = None
  def registerCelebornSkewShuffleCheckCallback(callback: function.Function[Integer, Boolean])
      : Unit = {
    celebornSkewShuffleCheckCallback = Some(callback)
  }

  // Initialize at the end of LifecycleManager construction.
  initialize()

  /**
   * A convenient method to stop [[RpcEndpoint]].
   */
  override def stop(): Unit = {
    heartbeater.stop()
    super.stop()
  }

  private def createSecret(): String = {
    val bits = 256
    val rnd = new SecureRandom()
    val secretBytes = new Array[Byte](bits / JByte.SIZE)
    rnd.nextBytes(secretBytes)
    JavaUtils.bytesToString(ByteBuffer.wrap(secretBytes))
  }

  def cancelAllActiveStages(reason: String): Unit = cancelShuffleCallback match {
    case Some(c) =>
      shuffleAllocatedWorkers
        .asScala
        .keys
        .filter(!commitManager.isStageEnd(_))
        .flatMap(shuffleId => Option(celebornShuffleIdToAppShuffleIdMap.get(shuffleId)))
        .toSet
        .foreach((shuffleId: Int) => c.accept(shuffleId, reason))
    case _ =>
  }

  def broadcastGetReducerFileGroupResponse(
      shuffleId: Int,
      response: GetReducerFileGroupResponse): Option[Array[Byte]] = {
    broadcastGetReducerFileGroupResponseCallback match {
      case Some(c) => Option(c.apply(shuffleId, response))
      case _ => None
    }
  }

  private def invalidatedBroadcastGetReducerFileGroupResponse(shuffleId: Int): Unit = {
    invalidatedBroadcastCallback match {
      case Some(c) => c.accept(shuffleId)
      case _ =>
    }
  }

  def computeFallbackCounts(
      fallbackCounts: ConcurrentHashMap[String, java.lang.Long],
      fallbackPolicy: String): Unit = {
    fallbackCounts.compute(
      fallbackPolicy,
      new BiFunction[String, java.lang.Long, java.lang.Long] {
        override def apply(k: String, v: java.lang.Long): java.lang.Long = {
          if (v == null) 1L else v + 1L
        }
      })
  }

  def getShuffleIdMapping = shuffleIdMapping

  // ---------------------------------------------------------------------------------------
  // Resume support (docs/LLD-resumable-spark-driver.md S6.2, patch 10). This LifecycleManager
  // is driver-embedded and holds no persistence of its own (S1.2): everything below exists to
  // let an operator-external anchor store seed a *fresh* instance -- running in a restarted
  // driver, for the same appUniqueId -- with the catalog a crashed instance held, without
  // re-deriving it from any commit traffic this instance never saw.
  // ---------------------------------------------------------------------------------------

  /**
   * Bind Spark's current shuffle ID to a durable semantic recovery key before map submission.
   * Exact replay is idempotent; changing the key in one driver is a correctness error.
   */
  def registerShuffleRecoveryIntent(appShuffleId: Int, recoveryKey: String): Unit = {
    require(appShuffleId >= 0, s"appShuffleId must be non-negative: $appShuffleId")
    require(recoveryKey != null && recoveryKey.nonEmpty, "recoveryKey must be non-empty")
    require(
      applicationLease != null,
      "An application lease must be acquired before registering a recovery intent")
    val existing = shuffleRecoveryKeys.putIfAbsent(appShuffleId, recoveryKey)
    if (existing != null && existing != recoveryKey) {
      throw new IllegalStateException(
        s"Spark shuffle $appShuffleId is already bound to recovery key $existing")
    }
  }

  private[client] def recoveryIdentity(celebornShuffleId: Int): Option[(Int, String)] = {
    Option(celebornShuffleIdToAppShuffleIdMap.get(celebornShuffleId)).flatMap { appShuffleId =>
      Option(shuffleRecoveryKeys.get(appShuffleId)).map(appShuffleId -> _)
    }
  }

  /**
   * Seed the catalog for one shuffle from state captured out of a previous LifecycleManager
   * instance (almost always: a crashed driver's). Must be called before any task for
   * appShuffleId runs in this driver -- afterwards handleGetShuffleIdForApp's normal writer
   * path would race it. Refuses to clobber a live registration: adoption only ever fills in
   * state this instance does not yet have (invariant A-1 -- when in doubt, do nothing rather
   * than overwrite).
   */
  private[celeborn] def adoptShuffle(
      appShuffleId: Int,
      appShuffleIdentifier: String,
      celebornShuffleId: Int,
      numMappers: Int,
      numPartitions: Int,
      fileGroups: util.Map[Integer, util.Set[PartitionLocation]],
      mapperAttempts: Array[Int]): Boolean = synchronized {
    require(appShuffleId >= 0, s"adoptShuffle: appShuffleId must be non-negative: $appShuffleId")
    require(
      appShuffleIdentifier != null && appShuffleIdentifier.nonEmpty,
      "adoptShuffle: appShuffleIdentifier must be non-empty")
    require(
      celebornShuffleId >= 0,
      s"adoptShuffle: celebornShuffleId must be non-negative: $celebornShuffleId")
    require(numMappers >= 0, s"adoptShuffle: numMappers must be non-negative: $numMappers")
    require(numPartitions > 0, s"adoptShuffle: numPartitions must be positive: $numPartitions")
    require(fileGroups != null, "adoptShuffle: fileGroups must not be null")
    require(mapperAttempts != null, "adoptShuffle: mapperAttempts must not be null")
    require(
      mapperAttempts.length == numMappers,
      s"adoptShuffle: mapperAttempts.length=${mapperAttempts.length} != numMappers=$numMappers")
    require(
      fileGroups.keySet().asScala.forall(id => id >= 0 && id < numPartitions),
      s"adoptShuffle: fileGroups contains a partition outside [0, $numPartitions)")
    require(
      fileGroups.values().asScala.forall(locs => locs != null && !locs.contains(null)),
      "adoptShuffle: fileGroups contains a null location set or location")

    // Validate every identity collision before touching the CommitHandler. Adoption publishes
    // state into several concurrent maps plus the handler's own catalog; a rejection after that
    // first mutation cannot be rolled back safely. Synchronizing this method makes the validation
    // and publication one transaction with respect to other adopters. Normal writer registration
    // still cannot race this call by contract: callers must adopt before submitting any task for
    // appShuffleId (documented above).
    if (shuffleIdMapping.containsKey(appShuffleId)) {
      logWarning(s"adoptShuffle: appShuffleId $appShuffleId already registered in this " +
        s"LifecycleManager, refusing to clobber it with adopted state")
      return false
    }
    val hasReverseMapping = celebornShuffleIdToAppShuffleIdMap.containsKey(celebornShuffleId)
    if (hasReverseMapping || registeredShuffle.contains(celebornShuffleId)) {
      val owner =
        if (hasReverseMapping) {
          s"for appShuffleId ${celebornShuffleIdToAppShuffleIdMap.get(celebornShuffleId)}"
        } else {
          "in this LifecycleManager"
        }
      logWarning(s"adoptShuffle: celebornShuffleId $celebornShuffleId is already registered " +
        s"$owner, refusing to alias it to appShuffleId $appShuffleId")
      return false
    }
    commitManager.getCommitHandler(celebornShuffleId).adoptCommittedShuffle(
      celebornShuffleId,
      numMappers,
      numPartitions,
      fileGroups,
      mapperAttempts)
    // LLD S8.1's advanceAtLeast watermark, applied to this instance's own shuffleIdGenerator
    // instead of Spark's nextShuffleId: without it, the next *unadopted* writer shuffle in this
    // driver could mint a celebornShuffleId that collides with one we just adopted, aliasing two
    // logically distinct shuffles onto one Celeborn file namespace with no exception anywhere.
    // A watermark bump can only waste identifiers, never reissue one already handed out.
    advanceShuffleIdGeneratorAtLeast(celebornShuffleId + 1)
    registeredShuffle.add(celebornShuffleId)
    celebornShuffleIdToAppShuffleIdMap.put(celebornShuffleId, appShuffleId)
    appShuffleDeterminateMap.put(appShuffleId, true)
    shuffleIdMapping.put(
      appShuffleId,
      scala.collection.mutable.LinkedHashMap(appShuffleIdentifier -> ((celebornShuffleId, true))))
    logInfo(s"adoptShuffle: appShuffleId=$appShuffleId appShuffleIdentifier=$appShuffleIdentifier " +
      s"-> celebornShuffleId=$celebornShuffleId ($numMappers mappers, $numPartitions partitions, " +
      s"${fileGroups.size()} partitions with file groups)")
    true
  }

  /**
   * Adopt a completed reduce shuffle using only Celeborn-authoritative physical metadata.
   *
   * The caller supplies semantic identity and a digest previously persisted with its recovery
   * anchor. Physical locations and mapper attempts are fetched from the current lease owner's
   * master and can never be injected by the anchor store. A successful return means every exact
   * worker file was validated and the shuffle is readable through this LifecycleManager.
   */
  def adoptShuffleFromCatalog(
      appShuffleId: Int,
      appShuffleIdentifier: String,
      recoveryKey: String,
      numMappers: Int,
      numPartitions: Int,
      expectedCatalogSha256: Array[Byte] = Array.emptyByteArray,
      probeTimeoutMs: Int = 2000): Option[LifecycleManager.AdoptedShuffleCatalog] = synchronized {
    require(
      recoveryKey != null && recoveryKey.nonEmpty,
      "adoptShuffleFromCatalog: recoveryKey must be non-empty")
    require(
      expectedCatalogSha256 != null &&
        (expectedCatalogSha256.isEmpty || expectedCatalogSha256.length == 32),
      "adoptShuffleFromCatalog: expectedCatalogSha256 must be empty or contain 32 bytes")
    Option(shuffleRecoveryKeys.get(appShuffleId)).foreach { existing =>
      require(
        existing == recoveryKey,
        s"Spark shuffle $appShuffleId is already bound to recovery key $existing")
    }

    val response = getCommittedShuffleCatalog(recoveryKey)
    if (!response.getSuccess) {
      throw new IllegalStateException(
        s"Unable to determine whether a committed catalog exists for recovery key " +
          s"$recoveryKey: ${response.getMessage}")
    }
    if (!response.getFound) {
      logWarning(s"No committed catalog exists for recovery key $recoveryKey: " +
        response.getMessage)
      return None
    }

    val catalogBytes = response.getCatalog.toByteArray
    val actualDigest = MessageDigest.getInstance("SHA-256").digest(catalogBytes)
    if (response.getSha256.size() != 32 ||
        !MessageDigest.isEqual(actualDigest, response.getSha256.toByteArray) ||
        (expectedCatalogSha256.nonEmpty &&
          !MessageDigest.isEqual(actualDigest, expectedCatalogSha256))) {
      throw new IllegalStateException(
        s"Committed catalog digest mismatch for recovery key $recoveryKey")
    }

    val catalog = try {
      PbCommittedShuffleCatalog.parseFrom(catalogBytes)
    } catch {
      case e: com.google.protobuf.InvalidProtocolBufferException =>
        throw new IllegalStateException(
          s"Malformed committed catalog for recovery key $recoveryKey", e)
    }
    val celebornShuffleId = catalog.getShuffleId
    if (catalog.getAppId != appUniqueId ||
        catalog.getRecoveryKey != recoveryKey ||
        catalog.getNumMappers != numMappers ||
        catalog.getNumPartitions != numPartitions ||
        catalog.getMapperAttemptsCount != numMappers ||
        catalog.getMapperAttemptsList.asScala.exists(_.intValue() < 0) ||
        catalog.getFileGroupsCount != numPartitions) {
      throw new IllegalStateException(
        s"Committed catalog identity or dimensions do not match recovery key $recoveryKey " +
          s"(original Spark shuffle ${catalog.getAppShuffleId}, current Spark shuffle " +
          s"$appShuffleId, Celeborn shuffle $celebornShuffleId)")
    }

    val fileGroups = new util.HashMap[Integer, util.Set[PartitionLocation]](numPartitions)
    val bytesByPartitionId = Array.fill[Long](numPartitions)(0L)
    catalog.getFileGroupsMap.asScala.foreach { case (partitionId, locationSet) =>
      if (partitionId < 0 || partitionId >= numPartitions ||
          locationSet.getLocationsCount == 0) {
        throw new IllegalStateException(
          s"Committed catalog contains an invalid reducer $partitionId for $recoveryKey")
      }
      val locations = new util.HashSet[PartitionLocation](locationSet.getLocationsCount)
      locationSet.getLocationsList.asScala.foreach { pbLocation =>
        if (pbLocation.getId != partitionId || !pbLocation.hasStorageInfo ||
            pbLocation.getStorageInfo.getFileSize < 0) {
          throw new IllegalStateException(
            s"Committed catalog contains an invalid location for reducer $partitionId " +
              s"for $recoveryKey")
        }
        locations.add(PbSerDeUtils.fromPbPartitionLocation(pbLocation))
        try {
          bytesByPartitionId(partitionId) = Math.addExact(
            bytesByPartitionId(partitionId),
            pbLocation.getStorageInfo.getFileSize)
        } catch {
          case _: ArithmeticException =>
            throw new IllegalStateException(
              s"Committed size overflows Long for reducer $partitionId for $recoveryKey")
        }
      }
      fileGroups.put(partitionId, locations)
    }
    if ((0 until numPartitions).exists(id => !fileGroups.containsKey(id))) {
      throw new IllegalStateException(
        s"Committed catalog is missing a reducer for recovery key $recoveryKey")
    }
    if (!confirmAlive(Map(celebornShuffleId -> fileGroups), probeTimeoutMs)
        .contains(celebornShuffleId)) {
      throw new IllegalStateException(
        s"Committed files are missing or unverifiable for recovery key $recoveryKey")
    }

    val mapperAttempts = catalog.getMapperAttemptsList.asScala.map(_.intValue()).toArray
    if (!adoptShuffle(
        appShuffleId,
        appShuffleIdentifier,
        celebornShuffleId,
        numMappers,
        numPartitions,
        fileGroups,
        mapperAttempts)) {
      throw new IllegalStateException(
        s"Driver-local state conflicts with committed recovery key $recoveryKey")
    } else {
      shuffleRecoveryKeys.put(appShuffleId, recoveryKey)
      Some(LifecycleManager.AdoptedShuffleCatalog(
        celebornShuffleId,
        bytesByPartitionId,
        actualDigest.clone()))
    }
  }

  private def advanceShuffleIdGeneratorAtLeast(floor: Int): Unit = {
    var cur = shuffleIdGenerator.get()
    while (cur < floor && !shuffleIdGenerator.compareAndSet(cur, floor))
      cur = shuffleIdGenerator.get()
  }

  /** Remove only driver-local state installed by catalog adoption; never delete worker data. */
  def rollbackAdoptedShuffle(appShuffleId: Int, celebornShuffleId: Int): Unit = synchronized {
    val mappings = shuffleIdMapping.get(appShuffleId)
    val ownsPhysicalShuffle = mappings != null &&
      mappings.values.exists(_._1 == celebornShuffleId) &&
      Option(celebornShuffleIdToAppShuffleIdMap.get(celebornShuffleId))
        .contains(appShuffleId)
    if (!ownsPhysicalShuffle) {
      throw new IllegalStateException(
        s"Shuffle $appShuffleId does not own adopted Celeborn shuffle $celebornShuffleId")
    }
    shuffleIdMapping.remove(appShuffleId)
    celebornShuffleIdToAppShuffleIdMap.remove(celebornShuffleId)
    registeredShuffle.remove(celebornShuffleId)
    appShuffleDeterminateMap.remove(appShuffleId)
    registeringShuffleRequest.remove(celebornShuffleId)
    shuffleAllocatedWorkers.remove(celebornShuffleId)
    latestPartitionLocation.remove(celebornShuffleId)
    commitManager.removeExpiredShuffle(celebornShuffleId)
    changePartitionManager.removeExpiredShuffle(celebornShuffleId)
    invalidatedBroadcastGetReducerFileGroupResponse(celebornShuffleId)
  }

  /**
   * Acceptance-ladder rung 7 (docs/LLD-resumable-spark-driver.md S3.6, invariant S-1): the
   * anchor store is never the authority, Celeborn is. A store TTL can outlive the data it
   * describes -- an application-heartbeat timeout reclaims worker-held files independently of
   * whatever the store still remembers -- so before adopting anything this instance did not
   * itself commit, ask every referenced worker to validate the exact committed file name, byte
   * length, and chunk-offset catalog captured in the anchor. Validation is read-only and does not
   * extend retention; adoption remains a separate operation after every worker has agreed.
   */
  def confirmAlive(
      celebornShuffleIdsWithFileGroups: Map[Int, util.Map[Integer, util.Set[PartitionLocation]]],
      probeTimeoutMs: Int = 2000): Set[Int] = {
    require(probeTimeoutMs > 0, s"probeTimeoutMs must be positive: $probeTimeoutMs")
    val timeout = new RpcTimeout(probeTimeoutMs.millis, "celeborn.resume.validation.timeout")

    def validateWorker(
        shuffleId: Int,
        host: String,
        rpcPort: Int,
        locations: Seq[PartitionLocation]): Boolean = {
      try {
        val endpoint = workerRpcEnvInUse.setupEndpointRefByAddr(
          RpcEndpointAddress(RpcAddress(host, rpcPort), WORKER_EP))
        val files = locations.map { location =>
          val storage = location.getStorageInfo
          require(
            storage != null,
            s"Shuffle $shuffleId location ${location.getUniqueId} has no committed storage info")
          ShuffleFileDescriptor(
            location.getFileName,
            storage.getFileSize,
            Option(storage.getChunkOffsets).getOrElse(util.Collections.emptyList()))
        }.asJava
        endpoint.askSync[ValidateShuffleFilesResponse](
          ValidateShuffleFiles(appUniqueId, shuffleId, files),
          timeout).status == StatusCode.SUCCESS
      } catch {
        case e: Exception =>
          logWarning(
            s"Exact recovery validation failed for shuffle $shuffleId on " +
              s"worker $host:$rpcPort",
            e)
          false
      }
    }

    celebornShuffleIdsWithFileGroups.collect {
      case (shuffleId, fileGroups) if !fileGroups.isEmpty && {
            val locations = fileGroups.values().asScala.flatMap(_.asScala).toSeq
            locations.nonEmpty && locations.groupBy(loc => (loc.getHost, loc.getRpcPort)).forall {
              case ((host, rpcPort), workerLocations) =>
                validateWorker(shuffleId, host, rpcPort, workerLocations)
            }
          } =>
        shuffleId
    }.toSet
  }
}
