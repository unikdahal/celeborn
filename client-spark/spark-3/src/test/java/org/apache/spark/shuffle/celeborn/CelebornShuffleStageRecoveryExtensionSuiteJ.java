/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.spark.shuffle.celeborn;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

/**
 * Recovery must refuse to enable when the client is not authenticated: with auth off, checkAuth on
 * the Celeborn services is a no-op and an unauthenticated peer could pre-publish task-commit
 * records for the application's recovery identity. See THREAT-MODEL.md §0 / requirement T-1.
 */
public class CelebornShuffleStageRecoveryExtensionSuiteJ {

  private static final String AUTH_KEY = "celeborn.auth.enabled";
  private static final String SPARK_AUTH_KEY = "spark.celeborn.auth.enabled";

  private static SparkConf recoveryConf() {
    return new SparkConf()
        .set(CelebornShuffleStageRecoveryExtension.ENABLED, "true")
        .set(CelebornShuffleStageRecoveryExtension.RECOVERY_ID, "recovery-under-test")
        .set(CelebornShuffleStageRecoveryExtension.STABLE_APP_ID, "app-under-test")
        .set(CelebornShuffleStageRecoveryExtension.LEASE_DURATION, "10m")
        .set(CelebornShuffleStageRecoveryExtension.PROBE_TIMEOUT, "2s");
  }

  private static SparkSession sessionOver(SparkConf conf) {
    SparkSession session = mock(SparkSession.class, RETURNS_DEEP_STUBS);
    when(session.sparkContext().getConf()).thenReturn(conf);
    return session;
  }

  /** Reflectively constructs the provider exactly as a SparkSession would during construction. */
  private static void constructProvider(SparkSession session) throws Throwable {
    Class<?> provider =
        Class.forName(CelebornShuffleStageRecoveryExtension.class.getName() + "$RecoveryProvider");
    Constructor<?> constructor = provider.getDeclaredConstructor(SparkSession.class);
    constructor.setAccessible(true);
    try {
      constructor.newInstance(session);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testRecoveryWithoutAuthenticationFailsAtConstruction() throws Throwable {
    try {
      constructProvider(sessionOver(recoveryConf()));
      Assert.fail("recovery without authentication must fail at session construction");
    } catch (IllegalStateException e) {
      Assert.assertTrue(
          "message must name the setting, got: " + e.getMessage(),
          e.getMessage().contains(AUTH_KEY));
      Assert.assertTrue(
          "message must name the Spark-side setting, got: " + e.getMessage(),
          e.getMessage().contains(SPARK_AUTH_KEY));
    }
  }

  @Test
  public void testAuthenticatedClientInstalls() throws Throwable {
    SparkConf conf = recoveryConf().set(SPARK_AUTH_KEY, "true");
    constructProvider(sessionOver(conf));
  }
}
