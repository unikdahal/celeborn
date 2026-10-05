# /*
#  * Licensed to the Apache Software Foundation (ASF) under one
#  * or more contributor license agreements.  See the NOTICE file
#  * distributed with this work for additional information
#  * regarding copyright ownership.  The ASF licenses this file
#  * to you under the Apache License, Version 2.0 (the
#  * "License"); you may not use this file except in compliance
#  * with the License.  You may obtain a copy of the License at
#  *
#  *   http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing,
#  * software distributed under the License is distributed on an
#  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  * KIND, either express or implied.  See the License for the
#  * specific language governing permissions and limitations
#  * under the License.
#  */
# 
import json
import statistics
from pathlib import Path

records = {}
for file in sorted(Path(".").glob("framing-benchmark-*.jsonl")):
    for line in file.read_text().splitlines():
        value = json.loads(line)
        records.setdefault((value["frame_bytes"], value["mode"]), []).append(value)
assert len(records) == 12
lines = ["# Framing allocation evidence", "",
         "Three independent Java 17 JVM forks, five warmups per size/mode, nine measured samples",
         "per fork. Payloads are pre-encoded direct buffers. This measures payload copying and",
         "framing only; encoding, retries, network, TLS, and kernel costs are excluded.", "",
         "Two-copy is a byte-array framing model; one-copy and direct exercise BufferPush and",
         "actual Netty reference retirement. These timings do not establish query or network speedups.", "",
         "| Payload bytes | Mode | Median of fork medians, ns/frame | Fork median range, ns/frame | Heap allocated bytes/frame |",
         "| ---: | --- | ---: | --- | ---: |"]
output = []
for (size, mode), values in sorted(records.items()):
    assert len(values) == 3
    medians = [item["median_ns_per_frame"] for item in values]
    allocation = statistics.median(item["heap_allocated_bytes_per_frame"] for item in values)
    value = {"frame_bytes": size, "mode": mode, "forks": 3,
             "median_ns_per_frame": statistics.median(medians),
             "fork_median_min_ns": min(medians), "fork_median_max_ns": max(medians),
             "heap_allocated_bytes_per_frame": allocation}
    output.append(value)
    lines.append(f"| {size} | {mode} | {value['median_ns_per_frame']:.1f} | {min(medians):.1f}–{max(medians):.1f} | {allocation:.1f} |")
Path("framing-report.json").write_text(json.dumps(output, indent=2))
Path("framing-report.md").write_text("\n".join(lines) + "\n")
print("\n".join(lines))
