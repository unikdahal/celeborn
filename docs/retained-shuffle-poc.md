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

## Provider-owned lifecycle process

`org.apache.celeborn.client.RetainedShuffleService` is a standalone entry point accepting
a Celeborn properties file and an unused endpoint output path. It starts the normal
Celeborn lifecycle manager and its master heartbeats outside Spark. The output properties
record application ID, service incarnation, host and port. Service restarts create a new
application identity. Shutdown closes leases and the lifecycle manager. The endpoint file
is only discovery metadata; clients must verify liveness and exact incarnation before use.

The process currently exposes the existing native lifecycle endpoint. The next changes
must connect producer/replacement clients to this owner, add the retention control RPCs,
and export a sealed commit descriptor. Merely launching the process is not yet an
end-to-end retention or recovery result. No provider restart/HA guarantee is claimed.

The service also registers `RetainedShuffleControlV1` in its native RPC environment.
Its internal JVM messages support probe, acquire, renew, and release; requests and
responses carry the exact service incarnation. Client code reuses an existing Celeborn
RPC environment and does not stop the owner. These messages use Celeborn's existing
JVM serialization fallback and are not a released language-neutral protocol. Identity
fencing is not a replacement for transport authorization. This control endpoint does not
seal output or establish worker availability. A client must measure any local lease
validity conservatively from the request start, not from the response arrival time.

## Sealed native read snapshot

The seal RPC requires a live retention lease, the exact registered mapper/reducer shape,
and an accepted mapper-attempt vector supplied by Spark. Only reduce-partition shuffles
are admitted. The commit handler requires stage completion without known data loss and
an exact winner-vector match. It refuses failed-batch repair metadata in this initial
slice. Stage completion alone is insufficient: Celeborn can mark a lost commit as ended.

The response owns an immutable copy of Celeborn's native reducer-file-group protobuf
payload. Its digest covers service incarnation, native shuffle ID, reducer count, mapper
winners, and payload. The client validates the response against its request and recomputes
the digest. Bounds currently limit mapper/reducer counts to 65536, partition locations
to 4096, and the serialized payload to 4 MiB. These are PoC limits, not production sizing.

A seal describes committed output at observation time. It is not an availability promise
beyond the lease, does not survive provider restart, and does not replace authorization.
Spark still needs to send its actual scheduler-accepted attempts and connect native reads
to the prepared adoption path. All new behavior awaits the combined validation phase.
