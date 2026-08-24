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

/**
 * Raised when a recovery request carries an application lease that is no longer current.
 *
 * This is a distinct type rather than a plain [[IllegalStateException]] so that callers and
 * metrics can separate "a newer driver has taken ownership", which is the protocol working as
 * intended, from "this request was malformed or exceeded a limit".
 */
class ApplicationLeaseFencedException(message: String) extends IllegalStateException(message)
