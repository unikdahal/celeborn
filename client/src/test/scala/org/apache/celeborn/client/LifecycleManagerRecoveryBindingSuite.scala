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

import org.apache.celeborn.CelebornFunSuite

class LifecycleManagerRecoveryBindingSuite extends CelebornFunSuite {

  test("source and write recovery bindings use stable disjoint namespaces") {
    assert(LifecycleManager.sourceRecoveryBindingId("iceberg:db.table") ===
      "source:v1:16:iceberg:db.table")
    assert(LifecycleManager.writeRecoveryBindingId("iceberg:db.table") ===
      "write:v1:16:iceberg:db.table")

    val adversarialSource = "write:v1:16:iceberg:db.table"
    assert(LifecycleManager.sourceRecoveryBindingId(adversarialSource) !==
      LifecycleManager.writeRecoveryBindingId("iceberg:db.table"))
  }

  test("length-delimited recovery bindings cannot alias concatenated identities") {
    val bindings = Seq(
      LifecycleManager.sourceRecoveryBindingId("a:b"),
      LifecycleManager.sourceRecoveryBindingId("a"),
      LifecycleManager.sourceRecoveryBindingId("b:a"),
      LifecycleManager.writeRecoveryBindingId("a:b"),
      LifecycleManager.writeRecoveryBindingId("a"),
      LifecycleManager.writeRecoveryBindingId("b:a"))

    assert(bindings.distinct.size === bindings.size)
  }
}
