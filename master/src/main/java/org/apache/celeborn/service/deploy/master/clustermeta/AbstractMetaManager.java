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

package org.apache.celeborn.service.deploy.master.clustermeta;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import scala.Option;
import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.ApplicationInfo;
import org.apache.celeborn.common.meta.ApplicationLease;
import org.apache.celeborn.common.meta.ApplicationMeta;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerEventInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.meta.WorkerStatus;
import org.apache.celeborn.common.network.CelebornRackResolver;
import org.apache.celeborn.common.protocol.PbSnapshotMetaInfo;
import org.apache.celeborn.common.protocol.PbWorkerStatus;
import org.apache.celeborn.common.quota.ResourceConsumption;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.PbSerDeUtils;
import org.apache.celeborn.common.util.RecoveryTaskCommitUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.common.util.WorkerStatusUtils;

/**
 * Note: Do not update the worker collections directly from outside the metadata manager, especially
 * {@link #workersMap}, {@link #workerEventInfos}, {@link #shutdownWorkers}, {@link
 * #excludedWorkers}, {@link #manuallyExcludedWorkers}, {@link #availableWorkers}.
 *
 * <p>All updates should be done through the provided methods to ensure consistency.
 */
public abstract class AbstractMetaManager implements IMetadataHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetaManager.class);

  // Metadata for master service
  public final Map<String, Set<Integer>> registeredAppAndShuffles =
      JavaUtils.newConcurrentHashMap();
  public final Set<String> hostnameSet = ConcurrentHashMap.newKeySet();
  public final Map<String, WorkerInfo> workersMap = JavaUtils.newConcurrentHashMap();
  public final Set<WorkerInfo> availableWorkers = ConcurrentHashMap.newKeySet();

  public final ConcurrentHashMap<WorkerInfo, Long> lostWorkers = JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<WorkerInfo, WorkerEventInfo> workerEventInfos =
      JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, Long> appHeartbeatTime = JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, ApplicationLease> applicationLeases =
      JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, Set<String>> applicationWorkers =
      JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, ByteString> committedShuffleCatalogs =
      JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, Integer> committedShuffleCatalogIndex =
      JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, String> sourceRecoveryAnchors =
      JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, ByteString> recoveryTaskCommits =
      JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, ByteString> recoveryBlobPointers =
      JavaUtils.newConcurrentHashMap();
  // Derived per-application shares of the inline budget: the sum of the per-recovery counters
  // over every live recovery of one application. Rebuilt on snapshot restore like the other
  // derived indexes and maintained incrementally by reserve/release, so the quota check is exact.
  private final ConcurrentHashMap<String, AtomicLong> recoveryTaskCommitBytesByApp =
      JavaUtils.newConcurrentHashMap();
  private final ConcurrentHashMap<String, AtomicLong> recoveryTaskCommitRecordsByApp =
      JavaUtils.newConcurrentHashMap();
  // How many pointers reference each (application, payload digest). Collection on a worker must
  // never delete a blob that any surviving pointer still names, and scanning every pointer at
  // collection time would be linear in the width of every live write.
  private final ConcurrentHashMap<String, AtomicLong> recoveryBlobDigestRefs =
      JavaUtils.newConcurrentHashMap();
  private final AtomicLong recoveryTaskCommitInlineBytes = new AtomicLong();
  private final AtomicLong recoveryTaskCommitInlineRecords = new AtomicLong();
  private final ConcurrentHashMap<String, AtomicLong> recoveryTaskCommitBytesByRecovery =
      JavaUtils.newConcurrentHashMap();
  private final ConcurrentHashMap<String, AtomicLong> recoveryTaskCommitRecordsByRecovery =
      JavaUtils.newConcurrentHashMap();
  public final Set<WorkerInfo> excludedWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> manuallyExcludedWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> shutdownWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> decommissionWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> workerLostEvents = ConcurrentHashMap.newKeySet();

  protected RpcEnv rpcEnv;
  protected CelebornConf conf;
  protected CelebornRackResolver rackResolver;

  public long initialEstimatedPartitionSize;
  public long estimatedPartitionSize;
  public double unhealthyDiskRatioThreshold;
  protected boolean autoReleaseHighWorkLoadEnabled;
  protected double autoReleaseHighWorkLoadRatioThreshold;
  protected boolean hasRemoteStorage;
  public final LongAdder partitionTotalWritten = new LongAdder();
  public final LongAdder partitionTotalFileCount = new LongAdder();
  public final LongAdder shuffleTotalCount = new LongAdder();
  public final LongAdder applicationTotalCount = new LongAdder();
  public final Map<String, Long> shuffleFallbackCounts = JavaUtils.newConcurrentHashMap();
  public final Map<String, Long> applicationFallbackCounts = JavaUtils.newConcurrentHashMap();

  public final ConcurrentHashMap<String, ApplicationInfo> applicationInfos =
      JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, ApplicationMeta> applicationMetas =
      JavaUtils.newConcurrentHashMap();

  public void updateApplicationInfo(
      String appId, UserIdentifier userIdentifier, Map<String, String> extraInfo) {
    applicationInfos.putIfAbsent(
        appId, new ApplicationInfo(appId, userIdentifier, extraInfo, System.currentTimeMillis()));
  }

  /**
   * Applies one lease transition replicated by the master metadata system.
   *
   * <p>The expected epoch is a compare-and-set fence. Replaying the exact transition is idempotent;
   * any competing or stale transition fails closed.
   */
  public synchronized ApplicationLease updateApplicationLeaseMeta(
      String appId, long expectedEpoch, long newEpoch, String ownerId, long expiresAtMs) {
    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Application id must be non-empty");
    }
    if (expectedEpoch < 0 || newEpoch != Math.addExact(expectedEpoch, 1L)) {
      throw new IllegalArgumentException("Application lease epoch must advance by exactly one");
    }

    ApplicationLease requested = new ApplicationLease(newEpoch, ownerId, expiresAtMs);
    ApplicationLease current = applicationLeases.get(appId);
    if (requested.equals(current)) {
      return current;
    }
    long currentEpoch = current == null ? 0L : current.epoch();
    if (currentEpoch != expectedEpoch) {
      throw new IllegalStateException(
          "Stale application lease transition for "
              + appId
              + ": expected epoch "
              + expectedEpoch
              + ", current epoch "
              + currentEpoch);
    }
    applicationLeases.put(appId, requested);
    return requested;
  }

  /**
   * Renews a lease without changing its fencing epoch.
   *
   * <p>The epoch and owner must still identify the current lease. Exact replay is idempotent, and
   * expiry may only move forward. This method intentionally does not consult wall-clock time so
   * that every metadata replica applies the same deterministic transition.
   */
  public synchronized ApplicationLease renewApplicationLeaseMeta(
      String appId, long epoch, String ownerId, long expiresAtMs) {
    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("Application id must be non-empty");
    }
    ApplicationLease current = applicationLeases.get(appId);
    if (current == null || current.epoch() != epoch || !current.ownerId().equals(ownerId)) {
      throw new IllegalStateException(
          "Application lease renewal is fenced for " + appId + " at epoch " + epoch);
    }
    if (expiresAtMs < current.expiresAtMs()) {
      throw new IllegalArgumentException("Application lease renewal cannot shorten expiry");
    }
    if (expiresAtMs == current.expiresAtMs()) {
      return current;
    }
    ApplicationLease renewed = new ApplicationLease(epoch, ownerId, expiresAtMs);
    applicationLeases.put(appId, renewed);
    return renewed;
  }

  public boolean hasValidApplicationLease(String appId, long epoch, String ownerId, long nowMs) {
    ApplicationLease lease = applicationLeases.get(appId);
    return lease != null && lease.isValid(epoch, ownerId, nowMs);
  }

  /** State-machine-order fencing check; wall-clock expiry is checked by the RPC leader. */
  public void requireApplicationLeaseOwnerMeta(String appId, long epoch, String ownerId) {
    ApplicationLease lease = applicationLeases.get(appId);
    if (lease == null || lease.epoch() != epoch || !lease.ownerId().equals(ownerId)) {
      throw new IllegalStateException(
          "Stale application lease owner for " + appId + " at epoch " + epoch);
    }
  }

  /**
   * Installs one fully committed shuffle catalog. Exact replay is idempotent; replacement is not.
   */
  public synchronized void updateCommittedShuffleCatalogMeta(
      String appId, int shuffleId, byte[] catalogBytes) {
    if (appId == null || appId.isEmpty() || shuffleId < 0 || catalogBytes == null) {
      throw new IllegalArgumentException("Invalid committed shuffle catalog identity or payload");
    }
    org.apache.celeborn.common.protocol.PbCommittedShuffleCatalog catalog;
    try {
      catalog =
          org.apache.celeborn.common.protocol.PbCommittedShuffleCatalog.parseFrom(catalogBytes);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Malformed committed shuffle catalog", e);
    }
    if (!appId.equals(catalog.getAppId()) || shuffleId != catalog.getShuffleId()) {
      throw new IllegalArgumentException("Committed shuffle catalog identity does not match key");
    }
    if (!catalog.getRecoveryKey().isEmpty() && catalog.getAppShuffleId() < 0) {
      throw new IllegalArgumentException("Committed shuffle catalog has an invalid Spark identity");
    }
    if (catalog.getNumMappers() < 0
        || catalog.getNumPartitions() <= 0
        || catalog.getMapperAttemptsCount() != catalog.getNumMappers()
        || catalog.getMapperAttemptsList().stream().anyMatch(attempt -> attempt < 0)
        || catalog.getFileGroupsCount() != catalog.getNumPartitions()
        || catalog.getFileGroupsMap().keySet().stream()
            .anyMatch(id -> id < 0 || id >= catalog.getNumPartitions())
        || catalog.getFileGroupsMap().entrySet().stream()
            .anyMatch(
                entry ->
                    entry.getValue().getLocationsCount() == 0
                        || entry.getValue().getLocationsList().stream()
                            .anyMatch(location -> location.getId() != entry.getKey()))) {
      throw new IllegalArgumentException("Committed shuffle catalog has inconsistent dimensions");
    }
    ByteString requested = ByteString.copyFrom(catalogBytes);
    String key = Utils.makeShuffleKey(appId, shuffleId);
    ByteString current = committedShuffleCatalogs.get(key);
    if (current != null && !current.equals(requested)) {
      throw new IllegalStateException("Committed shuffle catalog is immutable for " + key);
    }
    String recoveryIndexKey = null;
    if (!catalog.getRecoveryKey().isEmpty()) {
      recoveryIndexKey = committedCatalogRecoveryKey(appId, catalog.getRecoveryKey());
      Integer indexedShuffle = committedShuffleCatalogIndex.get(recoveryIndexKey);
      if (indexedShuffle != null && indexedShuffle != shuffleId) {
        throw new IllegalStateException(
            "Committed recovery key is immutable for " + catalog.getRecoveryKey());
      }
    }
    committedShuffleCatalogs.putIfAbsent(key, requested);
    if (recoveryIndexKey != null) {
      committedShuffleCatalogIndex.putIfAbsent(recoveryIndexKey, shuffleId);
    }
  }

  public ByteString getCommittedShuffleCatalog(String appId, int shuffleId, String recoveryKey) {
    int resolvedShuffleId = shuffleId;
    if (recoveryKey != null && !recoveryKey.isEmpty()) {
      Integer indexed =
          committedShuffleCatalogIndex.get(committedCatalogRecoveryKey(appId, recoveryKey));
      if (indexed == null) {
        return null;
      }
      resolvedShuffleId = indexed;
    }
    return committedShuffleCatalogs.get(Utils.makeShuffleKey(appId, resolvedShuffleId));
  }

  private static String committedCatalogRecoveryKey(String appId, String recoveryKey) {
    return appId.length() + ":" + appId + recoveryKey;
  }

  public String updateSourceRecoveryAnchorMeta(
      String appId, String recoveryId, String sourceId, String currentAnchor) {
    if (appId == null
        || appId.isEmpty()
        || recoveryId == null
        || recoveryId.isEmpty()
        || sourceId == null
        || sourceId.isEmpty()
        || currentAnchor == null
        || currentAnchor.isEmpty()) {
      throw new IllegalArgumentException("Invalid source recovery anchor identity or value");
    }
    String key = sourceRecoveryAnchorKey(appId, recoveryId, sourceId);
    String stored = sourceRecoveryAnchors.putIfAbsent(key, currentAnchor);
    return stored != null ? stored : currentAnchor;
  }

  public String getSourceRecoveryAnchor(String appId, String recoveryId, String sourceId) {
    return sourceRecoveryAnchors.get(sourceRecoveryAnchorKey(appId, recoveryId, sourceId));
  }

  private static String sourceRecoveryAnchorKey(String appId, String recoveryId, String sourceId) {
    return appId.length() + ":" + appId + recoveryId.length() + ":" + recoveryId + sourceId;
  }

  /**
   * Publishes an immutable commit for one logical output task and returns the canonical winner. An
   * exact retry is idempotent; a speculative attempt with different bytes loses without replacing
   * the first durable value.
   */
  public synchronized org.apache.celeborn.common.protocol.PbRecoveryTaskCommitRecord
      updateRecoveryTaskCommitMeta(
          String appId,
          String recoveryId,
          String writeId,
          int partitionId,
          byte[] payload,
          byte[] sha256) {
    validateRecoveryTaskCommitIdentity(appId, recoveryId, writeId, partitionId);
    RecoveryTaskCommitUtils.validatePayload(
        payload, sha256, conf.recoveryTaskCommitMaxPayloadSize());
    org.apache.celeborn.common.protocol.PbRecoveryTaskCommitRecord candidate =
        org.apache.celeborn.common.protocol.PbRecoveryTaskCommitRecord.newBuilder()
            .setAppId(appId)
            .setRecoveryId(recoveryId)
            .setWriteId(writeId)
            .setPartitionId(partitionId)
            .setPayload(ByteString.copyFrom(payload))
            .setSha256(ByteString.copyFrom(sha256))
            .build();
    String key = recoveryTaskCommitKey(appId, recoveryId, writeId, partitionId);
    ByteString stored = recoveryTaskCommits.get(key);
    if (stored != null) {
      return parseAndValidateRecoveryTaskCommit(stored, key);
    }
    ByteString serialized = candidate.toByteString();
    reserveRecoveryTaskCommitCapacity(appId, recoveryId, serialized.size(), 1L);
    recoveryTaskCommits.put(key, serialized);
    return candidate;
  }

  public synchronized org.apache.celeborn.common.protocol.PbRecoveryTaskCommitRecord
      getRecoveryTaskCommit(String appId, String recoveryId, String writeId, int partitionId) {
    validateRecoveryTaskCommitIdentity(appId, recoveryId, writeId, partitionId);
    String key = recoveryTaskCommitKey(appId, recoveryId, writeId, partitionId);
    ByteString stored = recoveryTaskCommits.get(key);
    return stored == null ? null : parseAndValidateRecoveryTaskCommit(stored, key);
  }

  private static void validateRecoveryTaskCommitIdentity(
      String appId, String recoveryId, String writeId, int partitionId) {
    RecoveryTaskCommitUtils.validateIdentity(appId, recoveryId, writeId, partitionId);
  }

  private static String recoveryTaskCommitKey(
      String appId, String recoveryId, String writeId, int partitionId) {
    return appId.length()
        + ":"
        + appId
        + recoveryId.length()
        + ":"
        + recoveryId
        + writeId.length()
        + ":"
        + writeId
        + partitionId;
  }

  private static String recoveryTaskCommitRecoveryKey(String appId, String recoveryId) {
    return appId.length() + ":" + appId + recoveryId.length() + ":" + recoveryId;
  }

  private void reserveRecoveryTaskCommitCapacity(
      String appId, String recoveryId, long bytes, long records) {
    String recoveryKey = recoveryTaskCommitRecoveryKey(appId, recoveryId);
    AtomicLong currentRecoveryBytes = recoveryTaskCommitBytesByRecovery.get(recoveryKey);
    AtomicLong currentRecoveryRecords = recoveryTaskCommitRecordsByRecovery.get(recoveryKey);
    long recoveryBytes = currentRecoveryBytes == null ? 0L : currentRecoveryBytes.get();
    long recoveryRecords = currentRecoveryRecords == null ? 0L : currentRecoveryRecords.get();
    long newRecoveryBytes = Math.addExact(recoveryBytes, bytes);
    long newRecoveryRecords = Math.addExact(recoveryRecords, records);
    // Per-application usage is the sum over that application's live recoveries. A wide or
    // repeatedly retried write from one application must not be able to consume the cluster-wide
    // budget and starve every other resumable write, so the share is checked before anything is
    // recorded. Rejections name which bound fired: per-recovery, per-application, or global.
    AtomicLong currentAppBytes = recoveryTaskCommitBytesByApp.get(appId);
    AtomicLong currentAppRecords = recoveryTaskCommitRecordsByApp.get(appId);
    long appBytes = currentAppBytes == null ? 0L : currentAppBytes.get();
    long appRecords = currentAppRecords == null ? 0L : currentAppRecords.get();
    long newAppBytes = Math.addExact(appBytes, bytes);
    long newAppRecords = Math.addExact(appRecords, records);
    long newGlobalBytes = Math.addExact(recoveryTaskCommitInlineBytes.get(), bytes);
    long newGlobalRecords = Math.addExact(recoveryTaskCommitInlineRecords.get(), records);
    if (newRecoveryBytes > conf.recoveryTaskCommitMaxInlineBytesPerRecovery()
        || newRecoveryRecords > conf.recoveryTaskCommitMaxInlineRecordsPerRecovery()) {
      throw new IllegalStateException(
          "Per-recovery inline metadata capacity exceeded for "
              + recoveryKey
              + "; use blob-backed storage or raise the maxInline*PerRecovery bounds");
    }
    if (newAppBytes > conf.recoveryTaskCommitMaxInlineBytesPerApp()
        || newAppRecords > conf.recoveryTaskCommitMaxInlineRecordsPerApp()) {
      throw new IllegalStateException(
          "Application "
              + appId
              + " exceeded its recovery inline metadata quota ("
              + conf.recoveryTaskCommitMaxInlineBytesPerApp()
              + " bytes / "
              + conf.recoveryTaskCommitMaxInlineRecordsPerApp()
              + " records); raise celeborn.master.recovery.taskCommit.maxInlineBytesPerApp or "
              + "reduce this application's concurrent resumable writes");
    }
    if (newGlobalBytes > conf.recoveryTaskCommitMaxInlineBytesGlobal()
        || newGlobalRecords > conf.recoveryTaskCommitMaxInlineRecordsGlobal()) {
      throw new IllegalStateException(
          "Cluster-wide recovery inline metadata capacity exceeded; raise the maxInline*Global "
              + "bounds or add master capacity");
    }
    recoveryTaskCommitBytesByRecovery
        .computeIfAbsent(recoveryKey, ignored -> new AtomicLong())
        .set(newRecoveryBytes);
    recoveryTaskCommitRecordsByRecovery
        .computeIfAbsent(recoveryKey, ignored -> new AtomicLong())
        .set(newRecoveryRecords);
    recoveryTaskCommitBytesByApp
        .computeIfAbsent(appId, ignored -> new AtomicLong())
        .set(newAppBytes);
    recoveryTaskCommitRecordsByApp
        .computeIfAbsent(appId, ignored -> new AtomicLong())
        .set(newAppRecords);
    recoveryTaskCommitInlineBytes.set(newGlobalBytes);
    recoveryTaskCommitInlineRecords.set(newGlobalRecords);
  }

  private void releaseRecoveryTaskCommitCapacity(
      org.apache.celeborn.common.protocol.PbRecoveryTaskCommitRecord record, int bytes) {
    releaseRecoveryTaskCommitCapacity(record.getAppId(), record.getRecoveryId(), bytes);
  }

  private void releaseRecoveryTaskCommitCapacity(String appId, String recoveryId, int bytes) {
    releaseRecoveryTaskCommitCapacity(appId, recoveryId, (long) bytes, 1L);
  }

  /**
   * Returns previously reserved inline budget. Bytes and records are adjusted independently so
   * paths that never touched the record count (a repair replacing replica lists) can release their
   * share without corrupting the record accounting.
   */
  private void releaseRecoveryTaskCommitCapacity(
      String appId, String recoveryId, long bytes, long records) {
    String recoveryKey = recoveryTaskCommitRecoveryKey(appId, recoveryId);
    if (bytes != 0) {
      recoveryTaskCommitInlineBytes.addAndGet(-bytes);
      recoveryTaskCommitBytesByRecovery.computeIfPresent(
          recoveryKey, (ignored, value) -> value.addAndGet(-bytes) == 0 ? null : value);
      recoveryTaskCommitBytesByApp.computeIfPresent(
          appId, (ignored, value) -> value.addAndGet(-bytes) == 0 ? null : value);
    }
    if (records != 0) {
      recoveryTaskCommitInlineRecords.addAndGet(-records);
      recoveryTaskCommitRecordsByRecovery.computeIfPresent(
          recoveryKey, (ignored, value) -> value.addAndGet(-records) == 0 ? null : value);
      recoveryTaskCommitRecordsByApp.computeIfPresent(
          appId, (ignored, value) -> value.addAndGet(-records) == 0 ? null : value);
    }
  }

  @VisibleForTesting
  public long recoveryTaskCommitInlineBytes() {
    return recoveryTaskCommitInlineBytes.get();
  }

  @VisibleForTesting
  public long recoveryTaskCommitInlineRecords() {
    return recoveryTaskCommitInlineRecords.get();
  }

  @VisibleForTesting
  public int recoveryTaskCommitRecoveryBuckets() {
    if (recoveryTaskCommitBytesByRecovery.size() != recoveryTaskCommitRecordsByRecovery.size()) {
      throw new IllegalStateException("Recovery task commit capacity indexes are inconsistent");
    }
    return recoveryTaskCommitBytesByRecovery.size();
  }

  /**
   * Publishes an immutable pointer to a worker-replicated recovery payload and returns the
   * canonical winner.
   *
   * <p>The payload never enters replicated state; only its content identity, length, and replica
   * locations do. Arbitration is unchanged from the inline backend: the first pointer under an
   * identity wins, an exact replay is idempotent, and a losing attempt receives the winner so it
   * can discard its own upload.
   */
  public synchronized org.apache.celeborn.common.protocol.PbRecoveryBlobPointer
      updateRecoveryBlobPointerMeta(
          String appId,
          String recoveryId,
          String writeId,
          int partitionId,
          byte[] sha256,
          long length,
          int formatVersion,
          List<String> workerIds,
          long createdAtMs) {
    validateRecoveryTaskCommitIdentity(appId, recoveryId, writeId, partitionId);
    validateBlobPointer(sha256, length, formatVersion, workerIds);

    String key = recoveryTaskCommitKey(appId, recoveryId, writeId, partitionId);
    ByteString stored = recoveryBlobPointers.get(key);
    if (stored != null) {
      return parseAndValidateRecoveryBlobPointer(stored, key);
    }

    org.apache.celeborn.common.protocol.PbRecoveryBlobPointer candidate =
        org.apache.celeborn.common.protocol.PbRecoveryBlobPointer.newBuilder()
            .setAppId(appId)
            .setRecoveryId(recoveryId)
            .setWriteId(writeId)
            .setPartitionId(partitionId)
            .setSha256(ByteString.copyFrom(sha256))
            .setLength(length)
            .setGeneration(0L)
            .setFormatVersion(formatVersion)
            .addAllWorkerIds(workerIds)
            .setCreatedAtMs(createdAtMs)
            .build();
    ByteString serialized = candidate.toByteString();
    // Pointers share the inline budget so that a flood of pointers cannot exhaust master state any
    // more than a flood of inline records could.
    reserveRecoveryTaskCommitCapacity(appId, recoveryId, serialized.size(), 1L);
    recoveryBlobPointers.put(key, serialized);
    retainRecoveryBlobDigest(appId, sha256);
    return candidate;
  }

  /**
   * Replaces the replica locations of an existing pointer after a repair.
   *
   * <p>The digest and length may never change, so a repair cannot alter what a recovery reads; the
   * generation must advance so that a stale repair cannot overwrite a newer replica set.
   */
  /**
   * Drops every durable record of one logical execution - task commits, blob pointers, and the
   * committed shuffle catalogs it published - returning the freed budget. Releasing an execution
   * that has nothing left is a no-op, so a client retry or a second successful driver is harmless.
   *
   * <p>Source anchors are deliberately retained: their identity is shared across executions that
   * read the same source, so removing them here could strand a concurrent sibling.
   */
  public synchronized long[] releaseRecoveryExecutionMeta(
      String appId, String recoveryId, java.util.List<String> recoveryKeys) {
    if (appId == null || appId.isEmpty() || recoveryId == null || recoveryId.isEmpty()) {
      throw new IllegalArgumentException(
          "Releasing a recovery execution requires an application and recovery identity");
    }
    String prefix = recoveryExecutionPrefix(appId, recoveryId);

    // Catalogs first: their index entries are derived from the records themselves.
    java.util.List<String> catalogKeys = new java.util.ArrayList<>();
    java.util.Map.Entry<String, ByteString> catalogHit = null;
    for (java.util.Map.Entry<String, ByteString> entry : committedShuffleCatalogs.entrySet()) {
      org.apache.celeborn.common.protocol.PbCommittedShuffleCatalog catalog;
      try {
        catalog =
            org.apache.celeborn.common.protocol.PbCommittedShuffleCatalog.parseFrom(
                entry.getValue());
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw new IllegalStateException("Malformed committed catalog in master state", e);
      }
      if (appId.equals(catalog.getAppId()) && recoveryKeys.contains(catalog.getRecoveryKey())) {
        catalogKeys.add(entry.getKey());
        catalogHit = entry;
      }
    }
    long releasedRecords = catalogKeys.size();
    long releasedPointers = 0;
    long releasedBytes = 0;
    for (String key : catalogKeys) {
      ByteString value = committedShuffleCatalogs.remove(key);
      if (value != null) {
        releaseRecoveryTaskCommitCapacity(appId, recoveryId, value.size(), 1L);
        releasedBytes += value.size();
      }
    }
    if (catalogHit != null) {
      try {
        org.apache.celeborn.common.protocol.PbCommittedShuffleCatalog catalog =
            org.apache.celeborn.common.protocol.PbCommittedShuffleCatalog.parseFrom(
                catalogHit.getValue());
        committedShuffleCatalogIndex
            .keySet()
            .removeIf(
                key -> key.equals(committedCatalogRecoveryKey(appId, catalog.getRecoveryKey())));
      } catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
        // already handled above
      }
    }

    java.util.List<String> commitKeys = new java.util.ArrayList<>();
    for (String key : recoveryTaskCommits.keySet()) {
      if (key.startsWith(prefix)) {
        commitKeys.add(key);
      }
    }
    for (String key : commitKeys) {
      ByteString value = recoveryTaskCommits.remove(key);
      if (value != null) {
        parseAndValidateRecoveryTaskCommit(value, key);
        releaseRecoveryTaskCommitCapacity(
            parseAndValidateRecoveryTaskCommit(value, key), value.size());
        releasedRecords++;
        releasedBytes += value.size();
      }
    }

    java.util.List<String> pointerKeys = new java.util.ArrayList<>();
    for (String key : recoveryBlobPointers.keySet()) {
      if (key.startsWith(prefix)) {
        pointerKeys.add(key);
      }
    }
    for (String key : pointerKeys) {
      ByteString value = recoveryBlobPointers.remove(key);
      if (value != null) {
        org.apache.celeborn.common.protocol.PbRecoveryBlobPointer pointer =
            parseAndValidateRecoveryBlobPointer(value, key);
        releaseRecoveryTaskCommitCapacity(
            pointer.getAppId(), pointer.getRecoveryId(), value.size());
        releaseRecoveryBlobDigest(pointer.getAppId(), pointer.getSha256().toByteArray());
        releasedPointers++;
        releasedBytes += value.size();
      }
    }
    return new long[] {releasedRecords, releasedBytes, releasedPointers};
  }

  private static String recoveryExecutionPrefix(String appId, String recoveryId) {
    return appId.length() + ":" + appId + recoveryId.length() + ":" + recoveryId;
  }

  public synchronized org.apache.celeborn.common.protocol.PbRecoveryBlobPointer
      repairRecoveryBlobPointerMeta(
          String appId,
          String recoveryId,
          String writeId,
          int partitionId,
          long generation,
          List<String> workerIds) {
    validateRecoveryTaskCommitIdentity(appId, recoveryId, writeId, partitionId);
    if (workerIds == null || workerIds.isEmpty()) {
      throw new IllegalArgumentException("A recovery blob pointer requires at least one replica");
    }

    String key = recoveryTaskCommitKey(appId, recoveryId, writeId, partitionId);
    ByteString stored = recoveryBlobPointers.get(key);
    if (stored == null) {
      throw new IllegalStateException("No recovery blob pointer exists for " + key);
    }

    org.apache.celeborn.common.protocol.PbRecoveryBlobPointer current =
        parseAndValidateRecoveryBlobPointer(stored, key);
    if (generation <= current.getGeneration()) {
      throw new IllegalStateException(
          "Stale recovery blob repair for "
              + key
              + ": generation "
              + generation
              + " does not advance "
              + current.getGeneration());
    }

    org.apache.celeborn.common.protocol.PbRecoveryBlobPointer repaired =
        current
            .toBuilder()
            .setGeneration(generation)
            .clearWorkerIds()
            .addAllWorkerIds(workerIds)
            .build();
    ByteString serialized = repaired.toByteString();
    long delta = serialized.size() - stored.size();
    if (delta > 0) {
      reserveRecoveryTaskCommitCapacity(appId, recoveryId, delta, 0L);
    } else if (delta < 0) {
      // A repair can shrink the pointer (shorter replica IDs): without returning the difference,
      // the per-recovery and global budgets only ever grow across repairs.
      releaseRecoveryTaskCommitCapacity(appId, recoveryId, -delta, 0L);
    }
    recoveryBlobPointers.put(key, serialized);
    return repaired;
  }

  public synchronized org.apache.celeborn.common.protocol.PbRecoveryBlobPointer
      getRecoveryBlobPointer(String appId, String recoveryId, String writeId, int partitionId) {
    validateRecoveryTaskCommitIdentity(appId, recoveryId, writeId, partitionId);
    String key = recoveryTaskCommitKey(appId, recoveryId, writeId, partitionId);
    ByteString stored = recoveryBlobPointers.get(key);
    return stored == null ? null : parseAndValidateRecoveryBlobPointer(stored, key);
  }

  /**
   * Every live pointer, parsed and validated.
   *
   * <p>Repair needs the whole set to find the ones that have fallen below their replication factor.
   * Parsing here rather than exposing the raw map keeps malformed replicated state from reaching a
   * caller that would treat it as a healthy pointer.
   */
  public synchronized java.util.List<org.apache.celeborn.common.protocol.PbRecoveryBlobPointer>
      allRecoveryBlobPointers() {
    java.util.List<org.apache.celeborn.common.protocol.PbRecoveryBlobPointer> pointers =
        new java.util.ArrayList<>(recoveryBlobPointers.size());
    recoveryBlobPointers.forEach(
        (key, value) -> pointers.add(parseAndValidateRecoveryBlobPointer(value, key)));
    return pointers;
  }

  private static String recoveryBlobDigestKey(String appId, byte[] sha256) {
    StringBuilder key = new StringBuilder(appId.length() + 1 + sha256.length * 2);
    key.append(appId.length()).append(':').append(appId);
    for (byte value : sha256) {
      key.append(Character.forDigit((value >> 4) & 0xf, 16));
      key.append(Character.forDigit(value & 0xf, 16));
    }
    return key.toString();
  }

  private void retainRecoveryBlobDigest(String appId, byte[] sha256) {
    recoveryBlobDigestRefs
        .computeIfAbsent(recoveryBlobDigestKey(appId, sha256), ignored -> new AtomicLong())
        .incrementAndGet();
  }

  private void releaseRecoveryBlobDigest(String appId, byte[] sha256) {
    recoveryBlobDigestRefs.computeIfPresent(
        recoveryBlobDigestKey(appId, sha256),
        (ignored, value) -> value.decrementAndGet() <= 0 ? null : value);
  }

  /**
   * Whether any live pointer still names this payload.
   *
   * <p>A worker asks this before collecting a blob it uploaded. An unreferenced answer is only safe
   * to act on after the orphan grace period, because a blob is uploaded before its pointer is
   * published and would otherwise be collected in that window.
   */
  public boolean isRecoveryBlobReferenced(String appId, byte[] sha256) {
    validateBlobDigest(sha256);
    AtomicLong references = recoveryBlobDigestRefs.get(recoveryBlobDigestKey(appId, sha256));
    return references != null && references.get() > 0L;
  }

  @VisibleForTesting
  public int recoveryBlobDigestCount() {
    return recoveryBlobDigestRefs.size();
  }

  private static void validateBlobDigest(byte[] sha256) {
    if (sha256 == null || sha256.length != 32) {
      throw new IllegalArgumentException("A recovery blob digest must be 32 bytes");
    }
  }

  private org.apache.celeborn.common.protocol.PbRecoveryBlobPointer
      parseAndValidateRecoveryBlobPointer(ByteString bytes, String expectedKey) {
    try {
      org.apache.celeborn.common.protocol.PbRecoveryBlobPointer pointer =
          org.apache.celeborn.common.protocol.PbRecoveryBlobPointer.parseFrom(bytes);
      validateRecoveryTaskCommitIdentity(
          pointer.getAppId(),
          pointer.getRecoveryId(),
          pointer.getWriteId(),
          pointer.getPartitionId());
      String key =
          recoveryTaskCommitKey(
              pointer.getAppId(),
              pointer.getRecoveryId(),
              pointer.getWriteId(),
              pointer.getPartitionId());
      if (!key.equals(expectedKey)) {
        throw new IllegalStateException(
            "Recovery blob pointer identity does not match its key " + expectedKey);
      }
      validateBlobPointer(
          pointer.getSha256().toByteArray(),
          pointer.getLength(),
          pointer.getFormatVersion(),
          pointer.getWorkerIdsList());
      return pointer;
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalStateException("Malformed recovery blob pointer in master state", e);
    }
  }

  private static void validateBlobPointer(
      byte[] sha256, long length, int formatVersion, List<String> workerIds) {
    if (sha256 == null || sha256.length != 32) {
      throw new IllegalArgumentException("A recovery blob pointer requires a 32-byte SHA-256");
    }
    if (length <= 0) {
      throw new IllegalArgumentException("A recovery blob pointer requires a positive length");
    }
    if (formatVersion <= 0) {
      throw new IllegalArgumentException(
          "A recovery blob pointer requires a positive format version");
    }
    if (workerIds == null || workerIds.isEmpty()) {
      throw new IllegalArgumentException("A recovery blob pointer requires at least one replica");
    }
  }

  private org.apache.celeborn.common.protocol.PbRecoveryTaskCommitRecord
      parseAndValidateRecoveryTaskCommit(ByteString bytes, String expectedKey) {
    try {
      org.apache.celeborn.common.protocol.PbRecoveryTaskCommitRecord record =
          org.apache.celeborn.common.protocol.PbRecoveryTaskCommitRecord.parseFrom(bytes);
      validateRecoveryTaskCommitIdentity(
          record.getAppId(), record.getRecoveryId(), record.getWriteId(), record.getPartitionId());
      if (!expectedKey.equals(
          recoveryTaskCommitKey(
              record.getAppId(),
              record.getRecoveryId(),
              record.getWriteId(),
              record.getPartitionId()))) {
        throw new IllegalStateException("Corrupt recovery task commit metadata for " + expectedKey);
      }
      RecoveryTaskCommitUtils.validatePayload(
          record.getPayload().toByteArray(),
          record.getSha256().toByteArray(),
          conf.recoveryTaskCommitMaxPayloadSize());
      return record;
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          "Malformed recovery task commit metadata for " + expectedKey, e);
    }
  }

  /**
   * Folds replicated blob pointers into the capacity state already computed for inline commits.
   *
   * <p>Pointers and inline records share one budget, so a restore must account for both. Restoring
   * a pointer without accounting for it would leave the counters below zero when the application is
   * later dropped, because cleanup releases capacity for every record it removes.
   */
  private RecoveryTaskCommitSnapshotState validateRecoveryBlobPointerSnapshot(
      Map<String, ByteString> pointers, RecoveryTaskCommitSnapshotState base) {
    Map<String, Long> bytesByRecovery = new HashMap<>(base.bytesByRecovery);
    Map<String, Long> recordsByRecovery = new HashMap<>(base.recordsByRecovery);
    Map<String, Long> bytesByApp = new HashMap<>(base.bytesByApp);
    Map<String, Long> recordsByApp = new HashMap<>(base.recordsByApp);
    long totalBytes = base.totalBytes;
    long totalRecords = base.totalRecords;
    for (Map.Entry<String, ByteString> entry : pointers.entrySet()) {
      org.apache.celeborn.common.protocol.PbRecoveryBlobPointer pointer =
          parseAndValidateRecoveryBlobPointer(entry.getValue(), entry.getKey());
      String recoveryKey =
          recoveryTaskCommitRecoveryKey(pointer.getAppId(), pointer.getRecoveryId());
      long recoveryBytes =
          Math.addExact(bytesByRecovery.getOrDefault(recoveryKey, 0L), entry.getValue().size());
      long recoveryRecords = Math.addExact(recordsByRecovery.getOrDefault(recoveryKey, 0L), 1L);
      long appBytes =
          Math.addExact(bytesByApp.getOrDefault(pointer.getAppId(), 0L), entry.getValue().size());
      long appRecords = Math.addExact(recordsByApp.getOrDefault(pointer.getAppId(), 0L), 1L);
      totalBytes = Math.addExact(totalBytes, entry.getValue().size());
      totalRecords = Math.addExact(totalRecords, 1L);
      if (recoveryBytes > conf.recoveryTaskCommitMaxInlineBytesPerRecovery()
          || recoveryRecords > conf.recoveryTaskCommitMaxInlineRecordsPerRecovery()
          || appBytes > conf.recoveryTaskCommitMaxInlineBytesPerApp()
          || appRecords > conf.recoveryTaskCommitMaxInlineRecordsPerApp()
          || totalBytes > conf.recoveryTaskCommitMaxInlineBytesGlobal()
          || totalRecords > conf.recoveryTaskCommitMaxInlineRecordsGlobal()) {
        throw new IllegalStateException(
            "Recovery blob pointer snapshot exceeds configured inline metadata capacity");
      }
      bytesByRecovery.put(recoveryKey, recoveryBytes);
      recordsByRecovery.put(recoveryKey, recoveryRecords);
      bytesByApp.put(pointer.getAppId(), appBytes);
      recordsByApp.put(pointer.getAppId(), appRecords);
    }
    return new RecoveryTaskCommitSnapshotState(
        bytesByRecovery, recordsByRecovery, bytesByApp, recordsByApp, totalBytes, totalRecords);
  }

  private RecoveryTaskCommitSnapshotState validateRecoveryTaskCommitSnapshot(
      Map<String, ByteString> records) {
    Map<String, Long> bytesByRecovery = new HashMap<>();
    Map<String, Long> recordsByRecovery = new HashMap<>();
    Map<String, Long> bytesByApp = new HashMap<>();
    Map<String, Long> recordsByApp = new HashMap<>();
    long totalBytes = 0L;
    long totalRecords = 0L;
    for (Map.Entry<String, ByteString> entry : records.entrySet()) {
      org.apache.celeborn.common.protocol.PbRecoveryTaskCommitRecord record =
          parseAndValidateRecoveryTaskCommit(entry.getValue(), entry.getKey());
      String recoveryKey = recoveryTaskCommitRecoveryKey(record.getAppId(), record.getRecoveryId());
      long recoveryBytes =
          Math.addExact(bytesByRecovery.getOrDefault(recoveryKey, 0L), entry.getValue().size());
      long recoveryRecords = Math.addExact(recordsByRecovery.getOrDefault(recoveryKey, 0L), 1L);
      long appBytes =
          Math.addExact(bytesByApp.getOrDefault(record.getAppId(), 0L), entry.getValue().size());
      long appRecords = Math.addExact(recordsByApp.getOrDefault(record.getAppId(), 0L), 1L);
      totalBytes = Math.addExact(totalBytes, entry.getValue().size());
      totalRecords = Math.addExact(totalRecords, 1L);
      if (recoveryBytes > conf.recoveryTaskCommitMaxInlineBytesPerRecovery()
          || recoveryRecords > conf.recoveryTaskCommitMaxInlineRecordsPerRecovery()
          || appBytes > conf.recoveryTaskCommitMaxInlineBytesPerApp()
          || appRecords > conf.recoveryTaskCommitMaxInlineRecordsPerApp()
          || totalBytes > conf.recoveryTaskCommitMaxInlineBytesGlobal()
          || totalRecords > conf.recoveryTaskCommitMaxInlineRecordsGlobal()) {
        throw new IllegalStateException(
            "Recovery task commit snapshot exceeds configured inline metadata capacity");
      }
      bytesByRecovery.put(recoveryKey, recoveryBytes);
      recordsByRecovery.put(recoveryKey, recoveryRecords);
      bytesByApp.put(record.getAppId(), appBytes);
      recordsByApp.put(record.getAppId(), appRecords);
    }
    return new RecoveryTaskCommitSnapshotState(
        bytesByRecovery, recordsByRecovery, bytesByApp, recordsByApp, totalBytes, totalRecords);
  }

  private static final class RecoveryTaskCommitSnapshotState {
    private final Map<String, Long> bytesByRecovery;
    private final Map<String, Long> recordsByRecovery;
    private final Map<String, Long> bytesByApp;
    private final Map<String, Long> recordsByApp;
    private final long totalBytes;
    private final long totalRecords;

    private RecoveryTaskCommitSnapshotState(
        Map<String, Long> bytesByRecovery,
        Map<String, Long> recordsByRecovery,
        Map<String, Long> bytesByApp,
        Map<String, Long> recordsByApp,
        long totalBytes,
        long totalRecords) {
      this.bytesByRecovery = bytesByRecovery;
      this.recordsByRecovery = recordsByRecovery;
      this.bytesByApp = bytesByApp;
      this.recordsByApp = recordsByApp;
      this.totalBytes = totalBytes;
      this.totalRecords = totalRecords;
    }
  }

  public void updateRequestSlotsMeta(
      String shuffleKey, String hostName, Map<String, Map<String, Integer>> workerWithAllocations) {
    Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
    registeredAppAndShuffles
        .computeIfAbsent(appIdShuffleId._1(), v -> new HashSet<>())
        .add((Integer) appIdShuffleId._2);

    String appId = appIdShuffleId._1;
    applicationWorkers
        .computeIfAbsent(appId, ignored -> ConcurrentHashMap.newKeySet())
        .addAll(workerWithAllocations.keySet());
    appHeartbeatTime.compute(
        appId,
        (applicationId, oldTimestamp) -> {
          long oldTime = System.currentTimeMillis();
          if (oldTimestamp != null) {
            oldTime = oldTimestamp;
          }
          return Math.max(System.currentTimeMillis(), oldTime);
        });

    if (hostName != null) {
      hostnameSet.add(hostName);
    }
  }

  public void updateUnregisterShuffleMeta(String shuffleKey) {
    Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
    Set<Integer> shuffleIds = registeredAppAndShuffles.get(appIdShuffleId._1());
    if (shuffleIds != null) {
      shuffleIds.remove(appIdShuffleId._2);
      registeredAppAndShuffles.compute(
          appIdShuffleId._1(),
          (s, shuffles) -> {
            if (shuffles.size() == 0) {
              return null;
            }
            return shuffles;
          });
    }
  }

  public void updateBatchUnregisterShuffleMeta(List<String> shuffleKeys) {
    for (String shuffleKey : shuffleKeys) {
      Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
      String appId = appIdShuffleId._1;
      if (registeredAppAndShuffles.containsKey(appId)) {
        registeredAppAndShuffles.get(appId).remove(appIdShuffleId._2);
      }
    }
  }

  public void updateAppHeartbeatMeta(
      String appId,
      long time,
      long totalWritten,
      long fileCount,
      long shuffleCount,
      long applicationCount,
      Map<String, Long> shuffleFallbackCounts,
      Map<String, Long> applicationFallbackCounts) {
    appHeartbeatTime.put(appId, time);
    partitionTotalWritten.add(totalWritten);
    partitionTotalFileCount.add(fileCount);
    shuffleTotalCount.add(shuffleCount);
    applicationTotalCount.add(applicationCount);
    addFallbackCounts(this.shuffleFallbackCounts, shuffleFallbackCounts);
    addFallbackCounts(this.applicationFallbackCounts, applicationFallbackCounts);
  }

  public synchronized void updateAppLostMeta(String appId) {
    registeredAppAndShuffles.remove(appId);
    appHeartbeatTime.remove(appId);
    applicationMetas.remove(appId);
    applicationInfos.remove(appId);
    applicationWorkers.remove(appId);
    committedShuffleCatalogs
        .entrySet()
        .removeIf(
            entry -> {
              try {
                return appId.equals(
                    org.apache.celeborn.common.protocol.PbCommittedShuffleCatalog.parseFrom(
                            entry.getValue())
                        .getAppId());
              } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new IllegalStateException("Malformed committed catalog in master state", e);
              }
            });
    String recoveryIndexPrefix = appId.length() + ":" + appId;
    committedShuffleCatalogIndex.keySet().removeIf(key -> key.startsWith(recoveryIndexPrefix));
    sourceRecoveryAnchors.keySet().removeIf(key -> key.startsWith(recoveryIndexPrefix));
    recoveryTaskCommits
        .entrySet()
        .removeIf(
            entry -> {
              if (entry.getKey().startsWith(recoveryIndexPrefix)) {
                releaseRecoveryTaskCommitCapacity(
                    parseAndValidateRecoveryTaskCommit(entry.getValue(), entry.getKey()),
                    entry.getValue().size());
                return true;
              }
              return false;
            });
    recoveryBlobPointers
        .entrySet()
        .removeIf(
            entry -> {
              if (entry.getKey().startsWith(recoveryIndexPrefix)) {
                org.apache.celeborn.common.protocol.PbRecoveryBlobPointer pointer =
                    parseAndValidateRecoveryBlobPointer(entry.getValue(), entry.getKey());
                releaseRecoveryTaskCommitCapacity(
                    pointer.getAppId(), pointer.getRecoveryId(), entry.getValue().size());
                releaseRecoveryBlobDigest(pointer.getAppId(), pointer.getSha256().toByteArray());
                return true;
              }
              return false;
            });
  }

  @VisibleForTesting
  public void updateExcludedWorkersMeta(
      List<WorkerInfo> workersToAdd, List<WorkerInfo> workersToRemove) {
    workersToAdd.forEach(
        worker -> {
          excludedWorkers.add(worker);
          availableWorkers.remove(worker);
        });
    workersToRemove.forEach(
        worker -> {
          excludedWorkers.remove(worker);
          updateAvailableWorkers(worker);
        });
  }

  public void updateManuallyExcludedWorkersMeta(
      List<WorkerInfo> workersToAdd, List<WorkerInfo> workersToRemove) {
    workersToAdd.forEach(
        worker -> {
          manuallyExcludedWorkers.add(worker);
          availableWorkers.remove(worker);
        });
    workersToRemove.forEach(
        worker -> {
          manuallyExcludedWorkers.remove(worker);
          updateAvailableWorkers(worker);
        });
  }

  public void reviseLostShuffles(String appId, List<Integer> lostShuffles) {
    registeredAppAndShuffles.computeIfAbsent(appId, v -> new HashSet<>()).addAll(lostShuffles);
  }

  public void deleteApp(String appId) {
    registeredAppAndShuffles.remove(appId);
  }

  public void updateWorkerLostMeta(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    WorkerInfo worker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
    workerLostEvents.add(worker);
    // remove worker from workers
    synchronized (workersMap) {
      workersMap.remove(worker.toUniqueId());
      lostWorkers.put(worker, System.currentTimeMillis());
      availableWorkers.remove(worker);
    }
    excludedWorkers.remove(worker);
    workerLostEvents.remove(worker);
  }

  public void removeWorkersUnavailableInfoMeta(List<WorkerInfo> unavailableWorkers) {
    synchronized (workersMap) {
      for (WorkerInfo workerInfo : unavailableWorkers) {
        if (lostWorkers.containsKey(workerInfo)) {
          lostWorkers.remove(workerInfo);
          shutdownWorkers.remove(workerInfo);
          workerEventInfos.remove(workerInfo);
          decommissionWorkers.remove(workerInfo);
          updateAvailableWorkers(workerInfo);
        }
      }
    }
  }

  private boolean hasAvailableStorage(WorkerInfo workerInfo) {
    Map<String, DiskInfo> disks = workerInfo.diskInfos();
    Pair<Boolean, Long> exceedCheckResult = isExceedingUnhealthyThreshold(disks);

    boolean hasDisk = !disks.isEmpty();
    boolean isExceeding = exceedCheckResult.getLeft();
    long unhealthyCount = exceedCheckResult.getRight();

    if (hasDisk) {
      if (!isExceeding) {
        return true;
      } else {
        LOG.warn(
            "Worker {} doesn't have enough healthy local disk (unhealthy count: {}). Has remote storage: {}",
            workerInfo,
            unhealthyCount,
            hasRemoteStorage);
      }
    }

    if (hasRemoteStorage) {
      return true;
    } else {
      LOG.warn("Worker {} has no available storage", workerInfo);
      return false;
    }
  }

  public void updateWorkerHeartbeatMeta(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Map<String, DiskInfo> disks,
      long time,
      WorkerStatus workerStatus,
      boolean highWorkload) {
    WorkerInfo worker =
        new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort, -1, disks, null);
    AtomicLong availableSlots = new AtomicLong();
    LOG.debug("update worker {}:{} heartbeat {}", host, rpcPort, disks);
    synchronized (workersMap) {
      Optional<WorkerInfo> workerInfo = Optional.ofNullable(workersMap.get(worker.toUniqueId()));
      workerInfo.ifPresent(
          info -> {
            info.updateThenGetDiskInfos(disks, Option.apply(estimatedPartitionSize));
            availableSlots.set(info.totalAvailableSlots());
            info.lastHeartbeat_$eq(time);
            info.setWorkerStatus(workerStatus);
            info.setWorkLoad(highWorkload);
          });
    }

    WorkerEventInfo workerEventInfo = workerEventInfos.get(worker);
    if (workerEventInfo != null
        && WorkerStatusUtils.meetFinalState(workerEventInfo, workerStatus)) {
      workerEventInfos.remove(worker);
      if (workerStatus.getState() == PbWorkerStatus.State.Normal) {
        shutdownWorkers.remove(worker);
      }
    }

    // If using HDFSONLY mode, workers with empty disks should not be put into excluded worker list.
    if (!excludedWorkers.contains(worker) && (!hasAvailableStorage(worker) || highWorkload)) {
      LOG.warn("Worker {} adds to excluded workers, high workload: {}", worker, highWorkload);
      excludedWorkers.add(worker);
    } else if ((availableSlots.get() > 0 || hasRemoteStorage) && !highWorkload) {
      // only unblack if numSlots larger than 0
      excludedWorkers.remove(worker);
    }

    // release high work load workers when too many excluded workers
    if (autoReleaseHighWorkLoadEnabled
        && excludedWorkers.size()
            >= Math.floor(workersMap.size() * autoReleaseHighWorkLoadRatioThreshold)) {
      synchronized (workersMap) {
        List<WorkerInfo> toRemoved =
            excludedWorkers.stream()
                .filter(
                    w -> {
                      WorkerInfo info = workersMap.get(w.toUniqueId());
                      return info != null
                          && info.isHighWorkLoad()
                          && hasAvailableStorage(w)
                          && info.totalAvailableSlots() > 0;
                    })
                .collect(Collectors.toList());
        updateExcludedWorkersMeta(new ArrayList<>(), toRemoved);
      }
    }

    // try to update the available workers if the worker status is Normal
    if (workerStatus.getState() == PbWorkerStatus.State.Normal) {
      updateAvailableWorkers(worker);
    }
  }

  public void updateRegisterWorkerMeta(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      int internalPort,
      String networkLocation,
      Map<String, DiskInfo> disks) {
    WorkerInfo workerInfo =
        new WorkerInfo(
            host,
            rpcPort,
            pushPort,
            fetchPort,
            replicatePort,
            internalPort,
            disks,
            new HashMap<>());
    workerInfo.lastHeartbeat_$eq(System.currentTimeMillis());
    if (networkLocation != null
        && !networkLocation.isEmpty()
        && !NetworkTopology.DEFAULT_RACK.equals(networkLocation)) {
      workerInfo.networkLocation_$eq(networkLocation);
    } else {
      workerInfo.networkLocation_$eq(rackResolver.resolve(host).getNetworkLocation());
    }
    workerInfo.updateDiskSlots(estimatedPartitionSize);
    synchronized (workersMap) {
      workersMap.putIfAbsent(workerInfo.toUniqueId(), workerInfo);
      shutdownWorkers.remove(workerInfo);
      lostWorkers.remove(workerInfo);
      excludedWorkers.remove(workerInfo);
      workerEventInfos.remove(workerInfo);
      decommissionWorkers.remove(workerInfo);
      updateAvailableWorkers(workerInfo);
    }
  }

  /**
   * Used for ratis state machine to take snapshot
   *
   * @param file
   * @throws IOException
   */
  public synchronized void writeMetaInfoToFile(File file) throws IOException, RuntimeException {
    byte[] snapshotBytes =
        PbSerDeUtils.toPbSnapshotMetaInfo(
                estimatedPartitionSize,
                registeredAppAndShuffles,
                hostnameSet,
                excludedWorkers,
                manuallyExcludedWorkers,
                workerLostEvents,
                appHeartbeatTime,
                applicationLeases,
                applicationWorkers,
                committedShuffleCatalogs,
                sourceRecoveryAnchors,
                recoveryTaskCommits,
                recoveryBlobPointers,
                new HashSet(workersMap.values()),
                partitionTotalWritten.sum(),
                partitionTotalFileCount.sum(),
                shuffleTotalCount.sum(),
                applicationTotalCount.sum(),
                shuffleFallbackCounts,
                applicationFallbackCounts,
                lostWorkers,
                shutdownWorkers,
                workerEventInfos,
                applicationMetas,
                applicationInfos,
                decommissionWorkers)
            .toByteArray();
    Files.write(file.toPath(), snapshotBytes);
  }

  /**
   * Used for ratis state machine to load snapshot
   *
   * @param file
   * @throws IOException
   */
  public synchronized void restoreMetaFromFile(File file) throws IOException {
    try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
      PbSnapshotMetaInfo snapshotMetaInfo = PbSnapshotMetaInfo.parseFrom(in);
      // Validate task-commit identities, checksums, keys, and all aggregate limits before clearing
      // live state. A corrupt or oversized snapshot must not leave a partially restored catalog.
      RecoveryTaskCommitSnapshotState taskCommitState =
          validateRecoveryBlobPointerSnapshot(
              snapshotMetaInfo.getRecoveryBlobPointersMap(),
              validateRecoveryTaskCommitSnapshot(snapshotMetaInfo.getRecoveryTaskCommitsMap()));
      cleanUpState();

      estimatedPartitionSize = snapshotMetaInfo.getEstimatedPartitionSize();

      for (String shuffleKey : snapshotMetaInfo.getRegisteredShuffleList()) {
        Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
        registeredAppAndShuffles
            .computeIfAbsent(appIdShuffleId._1, v -> new HashSet<>())
            .add((Integer) appIdShuffleId._2);
      }
      hostnameSet.addAll(snapshotMetaInfo.getHostnameSetList());
      excludedWorkers.addAll(
          snapshotMetaInfo.getExcludedWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));
      manuallyExcludedWorkers.addAll(
          snapshotMetaInfo.getManuallyExcludedWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));
      workerLostEvents.addAll(
          snapshotMetaInfo.getWorkerLostEventsList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));
      appHeartbeatTime.putAll(snapshotMetaInfo.getAppHeartbeatTimeMap());
      snapshotMetaInfo
          .getApplicationLeasesMap()
          .forEach(
              (appId, lease) ->
                  applicationLeases.put(
                      appId,
                      new ApplicationLease(
                          lease.getEpoch(), lease.getOwnerId(), lease.getExpiresAtMs())));
      snapshotMetaInfo
          .getApplicationWorkersMap()
          .forEach(
              (appId, workerIds) -> {
                Set<String> restored = ConcurrentHashMap.newKeySet(workerIds.getWorkerIdsCount());
                restored.addAll(workerIds.getWorkerIdsList());
                applicationWorkers.put(appId, restored);
              });
      committedShuffleCatalogs.putAll(snapshotMetaInfo.getCommittedShuffleCatalogsMap());
      sourceRecoveryAnchors.putAll(snapshotMetaInfo.getSourceRecoveryAnchorsMap());
      recoveryTaskCommits.putAll(snapshotMetaInfo.getRecoveryTaskCommitsMap());
      recoveryBlobPointers.putAll(snapshotMetaInfo.getRecoveryBlobPointersMap());
      // The index is derived state, so it is rebuilt from the pointers rather than replicated.
      recoveryBlobDigestRefs.clear();
      snapshotMetaInfo
          .getRecoveryBlobPointersMap()
          .forEach(
              (key, value) -> {
                org.apache.celeborn.common.protocol.PbRecoveryBlobPointer pointer =
                    parseAndValidateRecoveryBlobPointer(value, key);
                retainRecoveryBlobDigest(pointer.getAppId(), pointer.getSha256().toByteArray());
              });
      taskCommitState.bytesByRecovery.forEach(
          (key, value) -> recoveryTaskCommitBytesByRecovery.put(key, new AtomicLong(value)));
      taskCommitState.recordsByRecovery.forEach(
          (key, value) -> recoveryTaskCommitRecordsByRecovery.put(key, new AtomicLong(value)));
      taskCommitState.bytesByApp.forEach(
          (key, value) -> recoveryTaskCommitBytesByApp.put(key, new AtomicLong(value)));
      taskCommitState.recordsByApp.forEach(
          (key, value) -> recoveryTaskCommitRecordsByApp.put(key, new AtomicLong(value)));
      recoveryTaskCommitInlineBytes.set(taskCommitState.totalBytes);
      recoveryTaskCommitInlineRecords.set(taskCommitState.totalRecords);
      committedShuffleCatalogs
          .values()
          .forEach(
              bytes -> {
                try {
                  org.apache.celeborn.common.protocol.PbCommittedShuffleCatalog catalog =
                      org.apache.celeborn.common.protocol.PbCommittedShuffleCatalog.parseFrom(
                          bytes);
                  if (!catalog.getRecoveryKey().isEmpty()) {
                    committedShuffleCatalogIndex.put(
                        committedCatalogRecoveryKey(catalog.getAppId(), catalog.getRecoveryKey()),
                        catalog.getShuffleId());
                  }
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                  throw new IllegalStateException(
                      "Snapshot contains a malformed committed catalog", e);
                }
              });

      registeredAppAndShuffles.forEach(
          (appId, shuffleId) -> {
            if (!appHeartbeatTime.containsKey(appId)) {
              appHeartbeatTime.put(appId, System.currentTimeMillis());
            }
          });

      Set<WorkerInfo> workerInfoSet =
          snapshotMetaInfo.getWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet());
      List<String> workerHostList =
          workerInfoSet.stream()
              .filter(w -> NetworkTopology.DEFAULT_RACK.equals(w.networkLocation()))
              .map(WorkerInfo::host)
              .collect(Collectors.toList());
      scala.collection.immutable.Map<String, Node> resolveMap =
          rackResolver.resolveToMap(workerHostList);
      workersMap.putAll(
          workerInfoSet.stream()
              .peek(
                  workerInfo -> {
                    // Reset worker's network location with current master's configuration.
                    if (NetworkTopology.DEFAULT_RACK.equals(workerInfo.networkLocation())) {
                      workerInfo.networkLocation_$eq(
                          resolveMap.get(workerInfo.host()).get().getNetworkLocation());
                    }
                  })
              .collect(Collectors.toMap(WorkerInfo::toUniqueId, w -> w)));

      snapshotMetaInfo
          .getLostWorkersMap()
          .forEach((key, value) -> lostWorkers.put(WorkerInfo.fromUniqueId(key), value));

      snapshotMetaInfo
          .getWorkerEventInfosMap()
          .entrySet()
          .forEach(
              entry ->
                  workerEventInfos.put(
                      WorkerInfo.fromUniqueId(entry.getKey()),
                      PbSerDeUtils.fromPbWorkerEventInfo(entry.getValue())));

      shutdownWorkers.addAll(
          snapshotMetaInfo.getShutdownWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));

      decommissionWorkers.addAll(
          snapshotMetaInfo.getDecommissionWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));

      partitionTotalWritten.add(snapshotMetaInfo.getPartitionTotalWritten());
      partitionTotalFileCount.add(snapshotMetaInfo.getPartitionTotalFileCount());
      shuffleTotalCount.add(snapshotMetaInfo.getShuffleTotalCount());
      applicationTotalCount.add(snapshotMetaInfo.getApplicationTotalCount());
      addFallbackCounts(shuffleFallbackCounts, snapshotMetaInfo.getShuffleFallbackCountsMap());
      addFallbackCounts(
          applicationFallbackCounts, snapshotMetaInfo.getApplicationFallbackCountsMap());

      snapshotMetaInfo
          .getApplicationMetasMap()
          .forEach(
              (key, value) -> applicationMetas.put(key, PbSerDeUtils.fromPbApplicationMeta(value)));

      snapshotMetaInfo
          .getApplicationInfosMap()
          .forEach(
              (key, value) -> applicationInfos.put(key, PbSerDeUtils.fromPbApplicationInfo(value)));

      availableWorkers.addAll(
          workersMap.values().stream()
              .filter(worker -> isWorkerAvailable(worker))
              .collect(Collectors.toSet()));
    } catch (Exception e) {
      throw new IOException(e);
    }
    LOG.info("Successfully restore meta info from snapshot {}", file.getAbsolutePath());
    LOG.info(
        "Worker size: {}, Registered shuffle size: {}. Worker excluded list size: {}. Manually Excluded list size: {}",
        workersMap.size(),
        registeredAppAndShuffles.size(),
        excludedWorkers.size(),
        manuallyExcludedWorkers.size());
    workersMap.values().forEach(workerInfo -> LOG.info(workerInfo.toString()));
    registeredAppAndShuffles.forEach(
        (appId, shuffleId) -> LOG.info("RegisteredShuffle {}-{}", appId, shuffleId));
  }

  private void cleanUpState() {
    registeredAppAndShuffles.clear();
    hostnameSet.clear();
    workersMap.clear();
    availableWorkers.clear();
    lostWorkers.clear();
    appHeartbeatTime.clear();
    applicationLeases.clear();
    applicationWorkers.clear();
    committedShuffleCatalogs.clear();
    committedShuffleCatalogIndex.clear();
    sourceRecoveryAnchors.clear();
    recoveryTaskCommits.clear();
    recoveryBlobPointers.clear();
    recoveryBlobDigestRefs.clear();
    recoveryTaskCommitInlineBytes.set(0L);
    recoveryTaskCommitInlineRecords.set(0L);
    recoveryTaskCommitBytesByRecovery.clear();
    recoveryTaskCommitRecordsByRecovery.clear();
    recoveryTaskCommitBytesByApp.clear();
    recoveryTaskCommitRecordsByApp.clear();
    excludedWorkers.clear();
    shutdownWorkers.clear();
    decommissionWorkers.clear();
    manuallyExcludedWorkers.clear();
    workerLostEvents.clear();
    partitionTotalWritten.reset();
    partitionTotalFileCount.reset();
    shuffleTotalCount.reset();
    applicationTotalCount.reset();
    shuffleFallbackCounts.clear();
    applicationFallbackCounts.clear();
    workerEventInfos.clear();
    applicationMetas.clear();
    applicationInfos.clear();
  }

  public void updateMetaByReportWorkerUnavailable(List<WorkerInfo> failedWorkers) {
    synchronized (this.workersMap) {
      shutdownWorkers.addAll(failedWorkers);
      availableWorkers.removeAll(failedWorkers);
    }
  }

  public void updateWorkerEventMeta(int workerEventTypeValue, List<WorkerInfo> workerInfoList) {
    long eventTime = System.currentTimeMillis();
    ResourceProtos.WorkerEventType eventType =
        ResourceProtos.WorkerEventType.forNumber(workerEventTypeValue);
    synchronized (this.workersMap) {
      for (WorkerInfo workerInfo : workerInfoList) {
        WorkerEventInfo workerEventInfo = workerEventInfos.get(workerInfo);
        LOG.info("Received worker event: {} for worker: {}", eventType, workerInfo.toUniqueId());
        if (workerEventInfo == null || !workerEventInfo.isSameEvent(eventType.getNumber())) {
          if (eventType == ResourceProtos.WorkerEventType.None) {
            workerEventInfos.remove(workerInfo);
            updateAvailableWorkers(workerInfo);
          } else {
            workerEventInfos.put(workerInfo, new WorkerEventInfo(eventType.getNumber(), eventTime));
            availableWorkers.remove(workerInfo);
          }
        }
      }
    }
  }

  public void updateMetaByReportWorkerDecommission(List<WorkerInfo> workers) {
    synchronized (this.workersMap) {
      decommissionWorkers.addAll(workers);
      availableWorkers.removeAll(workers);
    }
  }

  public void updatePartitionSize() {
    long oldEstimatedPartitionSize = estimatedPartitionSize;
    long tmpTotalWritten = partitionTotalWritten.sumThenReset();
    long tmpFileCount = partitionTotalFileCount.sumThenReset();
    LOG.debug(
        "update partition size total written {}, file count {}",
        Utils.bytesToString(tmpTotalWritten),
        tmpFileCount);
    if (tmpFileCount != 0) {
      estimatedPartitionSize =
          Math.max(
              conf.minPartitionSizeToEstimate(),
              Math.min(tmpTotalWritten / tmpFileCount, conf.maxPartitionSizeToEstimate()));
    } else {
      estimatedPartitionSize = initialEstimatedPartitionSize;
    }

    // Do not trigger update is estimated partition size value is unchanged
    if (estimatedPartitionSize == oldEstimatedPartitionSize) {
      return;
    }

    LOG.warn(
        "Celeborn cluster estimated partition size changed from {} to {}",
        Utils.bytesToString(oldEstimatedPartitionSize),
        Utils.bytesToString(estimatedPartitionSize));

    HashSet<WorkerInfo> workers = new HashSet(workersMap.values());
    excludedWorkers.forEach(workers::remove);
    manuallyExcludedWorkers.forEach(workers::remove);
    workers.forEach(workerInfo -> workerInfo.updateDiskSlots(estimatedPartitionSize));
  }

  private boolean isWorkerAvailable(WorkerInfo workerInfo) {
    return (workerInfo.getWorkerStatus().getState() == PbWorkerStatus.State.Normal
            && !workerEventInfos.containsKey(workerInfo))
        && !excludedWorkers.contains(workerInfo)
        && !shutdownWorkers.contains(workerInfo)
        && !manuallyExcludedWorkers.contains(workerInfo);
  }

  private void updateAvailableWorkers(WorkerInfo worker) {
    synchronized (workersMap) {
      Optional<WorkerInfo> workerInfo = Optional.ofNullable(workersMap.get(worker.toUniqueId()));
      if (workerInfo.map(this::isWorkerAvailable).orElse(false)) {
        availableWorkers.add(workerInfo.get());
      } else {
        availableWorkers.remove(worker);
      }
    }
  }

  public void updateApplicationMeta(ApplicationMeta applicationMeta) {
    applicationMetas.putIfAbsent(applicationMeta.appId(), applicationMeta);
  }

  public void removeApplicationMeta(String appId) {
    applicationMetas.remove(appId);
  }

  public int registeredShuffleCount() {
    return registeredAppAndShuffles.values().stream().mapToInt(Set::size).sum();
  }

  private void addFallbackCounts(Map<String, Long> fallbackCounts, Map<String, Long> counts) {
    for (String fallbackPolicy : counts.keySet()) {
      fallbackCounts.compute(
          fallbackPolicy, (k, v) -> v == null ? counts.get(k) : v + counts.get(k));
    }
  }

  public void updateWorkerResourceConsumptions(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Map<UserIdentifier, ResourceConsumption> resourceConsumptions) {
    WorkerInfo worker =
        new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort, -1, null, null);
    synchronized (workersMap) {
      Optional<WorkerInfo> workerInfo = Optional.ofNullable(workersMap.get(worker.toUniqueId()));
      workerInfo.ifPresent(info -> info.updateThenGetUserResourceConsumption(resourceConsumptions));
    }
  }

  private Pair<Boolean, Long> isExceedingUnhealthyThreshold(Map<String, DiskInfo> diskMap) {
    long unhealthyCount = diskMap.values().stream().filter(disk -> !disk.isHealthy()).count();
    return new ImmutablePair<>(
        unhealthyCount * 1.0 / diskMap.size() >= unhealthyDiskRatioThreshold, unhealthyCount);
  }

  public boolean isAppInterruptShuffleEnabled(String appId) {
    return Boolean.parseBoolean(
        Optional.ofNullable(applicationInfos.get(appId))
            .map(ApplicationInfo::extraInfo)
            .map(extraInfo -> extraInfo.get(CelebornConf.QUOTA_INTERRUPT_SHUFFLE_ENABLED().key()))
            .orElse(CelebornConf.QUOTA_INTERRUPT_SHUFFLE_ENABLED().defaultValueString()));
  }
}
