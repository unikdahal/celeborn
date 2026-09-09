<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Retained shuffle PoC implementation

Branch `spip/retained-shuffle-poc` implements the Celeborn half of Spark's generic
cross-driver shuffle recovery experiment. Initial upstream base:
`749e45522a1726d25f5949d6ae82f1e30ac9f47e`.

The first implementation adds process-local, bounded leases to `LifecycleManager`.
A lease prevents delayed shuffle removal, including deletion of local reducer-location
and commit metadata. Acquire and cleanup fence the registered-shuffle set under one
local lock. Renewal cannot revive an expired token, and release checks the complete
token. Limits are initially 4096 leases and one hour per renewal. Stop invalidates
all leases. No network calls run while the lease lock is held.

These leases alone do not survive a Spark driver failure if the lifecycle manager
runs in that driver. The next implementation must make lifecycle ownership independent
of Spark attempts, export sealed native read metadata, and provide replacement clients
with fenced claims and reads. Worker/master loss and provider restart are not covered
by this process-local registry. A lease is not a seal or proof of successful commit.

Compilation, race tests, expiry tests, and end-to-end Spark integration are deferred
until the requested combined validation phase. Do not interpret this branch as a
validated provider integration yet.
