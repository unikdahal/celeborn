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
provider primitives for independent ownership and replacement reads. Spark publication
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

Spark publication/replacement adoption remains outstanding. Adding this
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
and recomputation path. The producer connection below supplies the native writer path;
the scheduler integration is still outstanding. Compilation and execution remain
deferred to the combined validation phase.

## Spark producer connection

The Spark shuffle manager recognizes the experimental setting
`spark.celeborn.retainedShuffle.endpointFile`. Set it to the discovery file written by
the existing standalone daemon, and continue using the normal Celeborn Spark shuffle
manager class. Only the driver reads the file. Executors receive the owner's application
ID, host, port, user identity, and reserved native shuffle ID in the serialized handle.
The normal driver-owned path remains the default when this property is absent.

The standalone owner reserves a distinct native ID for each `(producer UUID, Spark shuffle
ID)`. The client creates a fresh producer UUID per manager instance. Reservations use the
same native sequence as LifecycleManager's stage-rerun allocator, and repeated requests
must match the mapper/reducer shape. Spark's local shuffle ID remains unchanged for its
scheduler and executor cleanup tracking. Writers and readers resolve the separate native
ID through the handle. Use a dedicated owner for this protocol: legacy clients that write
unreserved native IDs must not share its application namespace.

The initial producer path requires `spark.celeborn.client.spark.shuffle.fallback.policy=NEVER`.
It rejects stage rerun, failed-shuffle cleaning, Celeborn skew optimization, reducer-file-group
broadcast, columnar shuffle, client authentication, and Spark IO encryption. Those features
need driver callbacks or data-format/lifetime integration that is not provided here. This
restriction concerns Celeborn's normal backend-selection policy; it does not implement or
replace Spark recovery's required recomputation fallback.

Unregister and producer shutdown retire reservations through the remote owner and shut down
only the producer's local control RPC environment. Retirement schedules normal delayed
shuffle cleanup; a live retention lease continues to pin registered metadata. A producer
must acquire its publication lease before unregister or shutdown. The publication API below pins output for handoff. Collection of
Spark's accepted encoded mapper attempts and renewal scheduling still need wiring.

Admission is limited to 4096 reservations over one owner lifetime, including retired
reservations. Tombstones prevent a retried registration from resurrecting a retired ID.
A producer crash does not currently reclaim its reservation table entries. These explicit
PoC limits must be replaced by bounded session expiry and catalog ownership before long-lived
shared deployment. The new producer code has not yet been compiled or exercised.

## Publication handoff and replacement entry point

`SparkShuffleManager.publishRetainedShuffle` delegates to the standalone producer using
Spark's local shuffle ID, the scheduler-frozen encoded attempt vector, and a handoff TTL.
It acquires a lease before sealing, requires the native winners to match, and emits a
bounded binary descriptor. Failed publication releases the lease. Successful publication
intentionally leaves that lease on the owner until its TTL expires, allowing producer
shutdown to retire the shuffle without immediately removing retained metadata. The TTL
starts at acquisition, not at descriptor persistence. Publication retries consume separate
bounded leases; this API is not an unlimited catalog retention promise.

The descriptor contains the owner route, user identity, application ID, incarnation, native
shuffle ID, shape, winner vector, native reducer metadata, and seal digest. Its decoder
checks format/version, bounded lengths, digest, and absence of trailing bytes. It contains
no lease token. The enclosing Spark manifest must bind these provider bytes to the certified
computation identity and atomically persist them; that integration is not yet wired.

`StandaloneRetainedShuffleReader` accepts those bytes and creates its own control RPC
environment and native reader. It must claim before the handoff pin expires or before
normal cleanup otherwise removes the output. Its new claim revalidates the seal against
the live owner. The caller owns renewal and close. Descriptor presence does not establish
liveness, and a constructor/read/renew failure must enter Spark's recovery miss or whole-
shuffle invalidation path. The codec regression suite is written but has not been run;
all new APIs still await compilation and the combined end-to-end validation.
