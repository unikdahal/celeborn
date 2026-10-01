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

/**
 * Pins the field-publication contract required by integrations that observe asynchronous push
 * completion through the retry executor.
 */
public class ShuffleClientIntegrationCompatibilitySuiteJ {

  @Test
  public void pushRetryPoolIsSafelyReplaceable() throws Exception {
    Field field = ShuffleClientImpl.class.getDeclaredField("pushDataRetryPool");
    int modifiers = field.getModifiers();

    assertFalse("pushDataRetryPool must be an instance field", Modifier.isStatic(modifiers));
    assertFalse("pushDataRetryPool must not be final", Modifier.isFinal(modifiers));
    assertTrue("pushDataRetryPool must be volatile", Modifier.isVolatile(modifiers));
  }
}
