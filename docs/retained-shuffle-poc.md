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
runs in that driver. The standalone extension and retained reader below provide the
provider primitives for independent ownership and replacement reads. Spark producer
and adoption integration remains outstanding. Worker/master loss and provider restart are not covered
by this process-local registry. A lease is not a seal or proof of successful commit.

Compilation, race tests, expiry tests, and end-to-end Spark integration are deferred
until the requested combined validation phase. Do not interpret this branch as a
validated provider integration yet.

## Existing standalone lifecycle daemon

The PoC extends the existing `LifecycleManagerDaemon` and uses its normal
`sbin/start-lifecycle-manager.sh` entry point. Add
`celeborn.retainedShuffle.endpointFile=/absolute/path/to/new-endpoint.properties`
to its properties file to attach retention and seal controls. With that setting absent,
the daemon remains unchanged. Its existing authentication restriction, command-line
configuration, master heartbeats, shutdown hook, and watchdog remain authoritative.
No second standalone process implementation is maintained.

The output properties record the daemon's application ID, a fresh retention-control
incarnation, host and port. Use a unique application ID for each new provider lifetime.
Even if an operator reuses the application ID, old descriptors must be rejected because
the control incarnation changes. This attachment does not restore metadata after restart.
The endpoint file is only discovery metadata, not evidence of liveness or a lease.

Spark producer/replacement integration remains outstanding. Adding this
attachment to the existing standalone daemon does not by itself prove end-to-end recovery.

The service also registers `RetainedShuffleControlV1` in its native RPC environment.
Its internal JVM messages support probe, acquire, renew, release, and seal; requests and
responses carry the exact service incarnation. Client code reuses an existing Celeborn
RPC environment and does not stop the owner. These messages use Celeborn's existing
JVM serialization fallback and are not a released language-neutral protocol. Identity
fencing is not a replacement for transport authorization. The control endpoint exports an already committed snapshot; it does not
commit output or establish worker availability. A client must measure any local lease
validity conservatively from the request start, not from the response arrival time.

## Sealed native read snapshot

The seal RPC requires a live retention lease, the exact registered mapper/reducer shape,
and an accepted mapper-attempt vector supplied by Spark. Only reduce-partition shuffles
are admitted. The commit handler requires stage completion without known data loss and
an exact winner-vector match. It refuses failed-batch repair metadata in this initial
slice. Stage completion alone is insufficient: Celeborn can mark a lost commit as ended.

The response owns an immutable copy of Celeborn's native reducer-file-group protobuf
payload. Its digest covers application ID, service incarnation, native shuffle ID, reducer count, mapper
winners, and payload. The client validates the response against its request and recomputes
the digest. Bounds currently limit mapper/reducer counts to 65536, partition locations
to 4096, and the serialized payload to 4 MiB. These are PoC limits, not production sizing.

A seal describes committed output at observation time. It is not an availability promise
beyond the lease, does not survive provider restart, and does not replace authorization.
Spark still needs to send its actual scheduler-accepted attempts and connect native reads
to the prepared adoption path. All new behavior awaits the combined validation phase.

## Native retained reader

`RetainedShuffleReader.claim` acquires a new lease from the recorded service incarnation
and requests a fresh seal using the stored mapper winners and reducer count. It admits
only an exact match with the stored seal. Application identity is included in the digest
because native worker reads use the owner's application ID, not the replacement driver's ID.

Reads use a dedicated native `ShuffleClientImpl`, sealed reducer locations, and the sealed
mapper-attempt vector. They do not refresh the reducer-file-group cache. The caller supplies
a reducer and map range; the reader rejects ranges outside the sealed shape. Stream reads
check the local lease before and after fetching bytes. Expiry fences the handle permanently;
a late renewal cannot revive it. Closing the reader closes its streams, its dedicated client,
and its lease without shutting down a compute application's shared client.

The local deadline starts before each acquire or renewal request. Renewal must run outside
fetch threads, and the caller is responsible for scheduling it. These checks bound local
use of a lease; they do not promise worker availability or instantaneous detection of a
provider failure. Native read errors still need to feed Spark's whole-shuffle invalidation
and recomputation path. Neither the Spark writer connection to the standalone owner nor
that scheduler integration is implemented by this reader. Compilation and execution remain
deferred to the combined validation phase.
