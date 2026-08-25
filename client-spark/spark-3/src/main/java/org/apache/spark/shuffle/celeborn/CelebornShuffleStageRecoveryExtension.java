/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.spark.shuffle.celeborn;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import scala.Option;
import scala.collection.mutable.ArrayBuffer;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.execution.SparkPlan;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.PbApplicationLeaseControlResponse;
import org.apache.celeborn.common.protocol.PbResolveSourceRecoveryAnchorResponse;

/**
 * Installs Celeborn's driver-recovery provider into Spark's optional shuffle-stage recovery SPI.
 * Reflection keeps ordinary Celeborn artifacts binary-compatible with Spark releases predating that
 * SPI; enabling recovery against such a release fails during session construction.
 */
public final class CelebornShuffleStageRecoveryExtension
    extends AbstractFunction1<SparkSessionExtensions, BoxedUnit> implements Serializable {

  static final String ENABLED = "spark.celeborn.driverRecovery.enabled";
  static final String RECOVERY_ID = "spark.celeborn.driverRecovery.id";
  static final String STABLE_APP_ID = "spark.celeborn.client.application.uniqueId";
  static final String LEASE_DURATION = "spark.celeborn.driverRecovery.leaseDuration";
  static final String PROBE_TIMEOUT = "spark.celeborn.driverRecovery.probeTimeout";

  /**
   * Recovery turns Celeborn's optional authentication into a correctness dependency: with
   * authentication off, {@code checkAuth} on the master and workers is a no-op, so an
   * unauthenticated peer that learns an application's recovery identity can pre-publish a
   * task-commit record and make the real writer discard its own output. Recovery therefore refuses
   * to install unless the client authenticates.
   */
  static void requireAuthenticatedClient(SparkConf conf) {
    if (!SparkUtils.fromSparkConf(conf).authEnabledOnClient()) {
      throw new IllegalStateException(
          "Celeborn driver recovery requires authentication: set "
              + CelebornConf.AUTH_ENABLED().key()
              + "=true (as spark.celeborn.auth.enabled in Spark). Without it, checkAuth on the"
              + " Celeborn master and workers accepts any peer, so an unauthenticated client could"
              + " publish task-commit records for this application's recovery identity");
    }
  }

  @Override
  public BoxedUnit apply(SparkSessionExtensions extensions) {
    Method injection =
        Arrays.stream(extensions.getClass().getMethods())
            .filter(method -> method.getName().equals("injectShuffleStageRecovery"))
            .findFirst()
            .orElse(null);
    if (injection == null) {
      throw new IllegalStateException(
          "Celeborn driver recovery requires a Spark build with ShuffleStageRecovery support");
    }
    try {
      injection.invoke(
          extensions,
          new AbstractFunction1<SparkSession, Object>() {
            @Override
            public Object apply(SparkSession session) {
              return new RecoveryProvider(session).proxy();
            }
          });
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to install Celeborn shuffle-stage recovery", e);
    }
    return BoxedUnit.UNIT;
  }

  private static final class RecoveryProvider implements InvocationHandler {
    private final SparkSession session;
    private final SparkConf conf;
    private final String recoveryId;
    private final String stableAppId;
    private final String ownerId = UUID.randomUUID().toString();
    private final long leaseDurationMs;
    private final int probeTimeoutMs;
    private final Map<Integer, Integer> adoptedShuffles = new ConcurrentHashMap<>();
    private final java.util.Set<String> registeredRecoveryKeys =
        java.util.Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final AtomicReference<RuntimeException> renewalFailure = new AtomicReference<>();

    private volatile LifecycleManager lifecycleManager;
    private volatile ScheduledExecutorService renewer;
    private volatile long leaseEpoch;

    private RecoveryProvider(SparkSession session) {
      this.session = session;
      this.conf = session.sparkContext().getConf();
      if (!conf.getBoolean(ENABLED, false)) {
        throw new IllegalStateException(
            "CelebornShuffleStageRecoveryExtension is configured but recovery is disabled");
      }
      requireAuthenticatedClient(conf);
      this.recoveryId = required(RECOVERY_ID);
      this.stableAppId = required(STABLE_APP_ID);
      this.leaseDurationMs = conf.getTimeAsMs(LEASE_DURATION, "10m");
      this.probeTimeoutMs = Math.toIntExact(conf.getTimeAsMs(PROBE_TIMEOUT, "2s"));
      if (leaseDurationMs <= 0 || probeTimeoutMs <= 0) {
        throw new IllegalArgumentException("Recovery lease and probe timeouts must be positive");
      }
    }

    private Object proxy() {
      try {
        Class<?> recoveryClass =
            Class.forName(
                "org.apache.spark.sql.execution.adaptive.ShuffleStageRecovery",
                true,
                session.getClass().getClassLoader());
        return Proxy.newProxyInstance(
            recoveryClass.getClassLoader(), new Class<?>[] {recoveryClass}, this);
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Spark ShuffleStageRecovery SPI is unavailable", e);
      }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      switch (method.getName()) {
        case "tryRecover":
          return tryRecover(args[0]);
        case "abortRecovery":
          abortRecovery(args[0]);
          return BoxedUnit.UNIT;
        case "onStageCompleted":
          checkRenewalFailure();
          return BoxedUnit.UNIT;
        case "resolveSourceAnchor":
          return resolveSourceAnchor(args[0]);
        case "resolveWriteId":
          return resolveWriteId(args[0]);
        case "toString":
          return "CelebornShuffleStageRecovery(" + recoveryId + ")";
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        default:
          throw new UnsupportedOperationException(
              "Unsupported ShuffleStageRecovery method " + method.getName());
      }
    }

    private Object tryRecover(Object info) throws Exception {
      checkRenewalFailure();
      LifecycleManager manager = lifecycleManager();
      int appShuffleId = intProperty(info, "shuffleId");
      int numMappers = intProperty(info, "numMappers");
      int numPartitions = intProperty(info, "numPartitions");
      String recoveryKey = recoveryKey(info, numMappers, numPartitions);

      manager.registerShuffleRecoveryIntent(appShuffleId, recoveryKey);
      registeredRecoveryKeys.add(recoveryKey);
      Option<LifecycleManager.AdoptedShuffleCatalog> adopted =
          manager.adoptShuffleFromCatalog(
              appShuffleId,
              recoveryKey,
              recoveryKey,
              numMappers,
              numPartitions,
              new byte[0],
              probeTimeoutMs);
      if (adopted.isEmpty()) {
        return Option.empty();
      }

      LifecycleManager.AdoptedShuffleCatalog catalog = adopted.get();
      adoptedShuffles.put(appShuffleId, catalog.celebornShuffleId());
      Object recovered = recoveredShuffleStage(catalog.bytesByPartitionId());
      return Option.apply(recovered);
    }

    private String resolveSourceAnchor(Object info) {
      checkRenewalFailure();
      String sourceId = stringProperty(info, "sourceId");
      String currentAnchor = stringProperty(info, "currentAnchor");
      PbResolveSourceRecoveryAnchorResponse response =
          lifecycleManager().resolveSourceRecoveryAnchor(recoveryId, sourceId, currentAnchor);
      if (!response.getSuccess()) {
        throw new IllegalStateException(
            "Unable to resolve durable source anchor: " + response.getMessage());
      }
      String anchor = response.getAnchor();
      if (anchor == null || anchor.isEmpty()) {
        throw new IllegalStateException("Celeborn returned an empty durable source anchor");
      }
      return anchor;
    }

    private String resolveWriteId(Object info) {
      checkRenewalFailure();
      String sinkId = stringProperty(info, "sinkId");
      String currentWriteId = stringProperty(info, "currentWriteId");
      PbResolveSourceRecoveryAnchorResponse response =
          lifecycleManager().resolveWriteRecoveryId(recoveryId, sinkId, currentWriteId);
      if (!response.getSuccess()) {
        throw new IllegalStateException(
            "Unable to resolve durable write ID: " + response.getMessage());
      }
      String writeId = response.getAnchor();
      if (writeId == null || writeId.isEmpty()) {
        throw new IllegalStateException("Celeborn returned an empty durable write ID");
      }
      return writeId;
    }

    private void abortRecovery(Object info) {
      int appShuffleId = intProperty(info, "shuffleId");
      Integer celebornShuffleId = adoptedShuffles.remove(appShuffleId);
      if (celebornShuffleId != null) {
        lifecycleManager.rollbackAdoptedShuffle(appShuffleId, celebornShuffleId);
      }
    }

    private synchronized LifecycleManager lifecycleManager() {
      if (lifecycleManager != null) {
        return lifecycleManager;
      }
      if (!(session.sparkContext().env().shuffleManager() instanceof SparkShuffleManager)) {
        throw new IllegalStateException("Celeborn driver recovery requires SparkShuffleManager");
      }
      SparkShuffleManager shuffleManager =
          (SparkShuffleManager) session.sparkContext().env().shuffleManager();
      LifecycleManager candidate = shuffleManager.getLifecycleManager();
      if (candidate == null) {
        throw new IllegalStateException("Celeborn LifecycleManager is not initialized");
      }
      if (!stableAppId.equals(candidate.appUniqueId())) {
        throw new IllegalStateException(
            "Celeborn app identity is "
                + candidate.appUniqueId()
                + ", expected stable recovery identity "
                + stableAppId);
      }
      PbApplicationLeaseControlResponse acquired =
          candidate.takeOverApplicationLease(ownerId, leaseDurationMs);
      if (!acquired.getSuccess()) {
        throw new IllegalStateException(
            "Unable to acquire Celeborn recovery lease: " + acquired.getMessage());
      }
      leaseEpoch = acquired.getEpoch();
      lifecycleManager = candidate;
      startRenewer();
      return candidate;
    }

    private void startRenewer() {
      ThreadFactory factory =
          runnable -> {
            Thread thread = new Thread(runnable, "celeborn-recovery-lease-renewer");
            thread.setDaemon(true);
            return thread;
          };
      renewer = Executors.newSingleThreadScheduledExecutor(factory);
      long intervalMs = Math.max(1000L, leaseDurationMs / 3L);
      renewer.scheduleWithFixedDelay(
          () -> {
            try {
              PbApplicationLeaseControlResponse renewed =
                  lifecycleManager.renewApplicationLeaseForDuration(
                      leaseEpoch, ownerId, leaseDurationMs);
              if (!renewed.getSuccess()) {
                throw new IllegalStateException(renewed.getMessage());
              }
              leaseEpoch = renewed.getEpoch();
            } catch (RuntimeException e) {
              renewalFailure.compareAndSet(null, e);
              session.sparkContext().cancelAllJobs();
            }
          },
          intervalMs,
          intervalMs,
          TimeUnit.MILLISECONDS);
      session
          .sparkContext()
          .addSparkListener(
              new SparkListener() {
                @Override
                public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
                  renewer.shutdownNow();
                }
              });
    }

    private Object recoveredShuffleStage(long[] bytesByPartitionId) throws Exception {
      ArrayBuffer<Object> bytes = new ArrayBuffer<>(bytesByPartitionId.length);
      long dataSize = 0L;
      for (long size : bytesByPartitionId) {
        bytes.$plus$eq(Long.valueOf(size));
        dataSize = Math.addExact(dataSize, size);
      }
      Class<?> recoveredClass =
          Class.forName(
              "org.apache.spark.sql.execution.adaptive.RecoveredShuffleStage",
              true,
              session.getClass().getClassLoader());
      return recoveredClass.getConstructors()[0].newInstance(
          bytes.toSeq(), dataSize, Option.empty());
    }

    private String recoveryKey(Object info, int numMappers, int numPartitions) throws Exception {
      SparkPlan canonicalized =
          (SparkPlan) info.getClass().getMethod("canonicalizedPlan").invoke(info);
      SparkPlan canonicalizedQuery =
          (SparkPlan) info.getClass().getMethod("canonicalizedQueryPlan").invoke(info);
      String material =
          session.version()
              + '\n'
              + recoveryId
              + '\n'
              + numMappers
              + '\n'
              + numPartitions
              + '\n'
              + canonicalizedQuery.treeString()
              + '\n'
              + canonicalized.treeString();
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(material.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(digest.length * 2);
      for (byte value : digest) {
        hex.append(String.format("%02x", value & 0xff));
      }
      return recoveryId + "/" + hex;
    }

    private int intProperty(Object value, String name) {
      try {
        return ((Number) value.getClass().getMethod(name).invoke(value)).intValue();
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("Invalid ShuffleStageRecoveryInfo." + name, e);
      }
    }

    private String stringProperty(Object value, String name) {
      try {
        Object result = value.getClass().getMethod(name).invoke(value);
        if (!(result instanceof String) || ((String) result).isEmpty()) {
          throw new IllegalStateException("Empty ShuffleStageRecovery source property " + name);
        }
        return (String) result;
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("Invalid SourceRecoveryInfo." + name, e);
      }
    }

    private String required(String key) {
      String value = conf.get(key, "").trim();
      if (value.isEmpty()) {
        throw new IllegalArgumentException("Missing required recovery configuration " + key);
      }
      return value;
    }

    private void checkRenewalFailure() {
      RuntimeException failure = renewalFailure.get();
      if (failure != null) {
        throw new IllegalStateException("Celeborn recovery lease renewal failed", failure);
      }
    }
  }
}
