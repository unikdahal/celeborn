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

package org.apache.celeborn.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.junit.Test;

import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.client.TransportResponseHandler;

/**
 * Pins the safely replaceable fields used by Comet to track native Celeborn push completion.
 *
 * <p>Comet replaces these references after construction. They must remain volatile instance fields
 * so transport and retry threads observe the replacement.
 */
public class CometNativeShuffleCompatibilitySuiteJ {

  @Test
  public void completionTrackingFieldsAreSafelyReplaceable() throws Exception {
    assertSafelyReplaceable(TransportClientFactory.class, "clientBootstraps");
    assertSafelyReplaceable(ShuffleClientImpl.class, "pushDataRetryPool");
    assertSafelyReplaceable(TransportClient.class, "channel");
    assertSafelyReplaceable(TransportResponseHandler.class, "outstandingPushes");
  }

  private static void assertSafelyReplaceable(Class<?> owner, String fieldName) throws Exception {
    Field field = owner.getDeclaredField(fieldName);
    int modifiers = field.getModifiers();

    assertFalse(
        owner.getName() + "." + fieldName + " must be an instance field",
        Modifier.isStatic(modifiers));
    assertFalse(
        owner.getName() + "." + fieldName + " must not be final", Modifier.isFinal(modifiers));
    assertTrue(
        owner.getName() + "." + fieldName + " must be volatile", Modifier.isVolatile(modifiers));
  }
}
