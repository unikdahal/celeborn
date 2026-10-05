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

# Caller-owned buffer pushes

`ShuffleClient.pushDataAsync` accepts the remaining bytes of a heap, direct,
sliced or read-only `ByteBuffer`. The caller supplies already encoded data.
The call preserves its position and limit, skips Celeborn compression, and
adds a separate 16-byte batch header. The existing push and retry machinery
handles partition locations, congestion, splits, replication and mapper state.
The ordinary byte-array push and merge APIs keep their compression and
shuffle-encryption behavior.

The caller must keep the bytes immutable and their allocation live until
completion, including exceptional completion. A successful RPC response or
an empty in-flight counter is insufficient. The returned completion stage
waits for the final logical outcome, active callbacks, queued/running retries,
and the final reference to each Netty request body. A failed/timed-out write
may retain its body after its RPC callback. Cleanup cancels the map's logical
pushes but does not release a live transport buffer.

The submitting invocation and each retry/callback hold work references.
Each composite request body holds another reference, returned by its final
Netty deallocation. A terminal operation refuses new work references. Its
completion promise resolves only after all held references retire. Completion
also detaches its payload references so late callbacks do not keep heap buffers
alive. Late callbacks cannot acquire work or access the retired payload.

A caller may cancel its dependent future, but that does not cancel a push or
prove its allocation can be freed. Use map cleanup for cancellation and do not
cancel the lifetime future when deciding whether to release memory. Client
shutdown cancels outstanding buffer pushes before closing transports and
letting queued retry work retire.

Integrity accounting covers the input bytes once per logical push, before
framing or retry. Encryption is unsupported by this raw API. Check
`supportsBufferPush()` and use the ordinary byte-array API when it is false.
The client does not remove or bypass an installed encryption handler.
Transport TLS can still encrypt its own outbound buffers; the no-copy claim
covers Celeborn payload framing, not TLS or kernel networking.

The completed integer is the framed batch length, or zero if the mapper
already ended. Synchronous argument/security validation errors leave ownership
with the caller. Once work is accepted, errors are reported through the stage
after every buffer reader has retired.

The wire batch format is unchanged. Clients can use this API with compatible
existing servers; driver/executor classpaths must contain the same client build.
The API is optional in the base client and defaults to unsupported so existing
client implementations remain binary compatible.
