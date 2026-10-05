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

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;

import io.netty.buffer.ByteBuf;

import org.apache.celeborn.common.network.buffer.ManagedBuffer;

/** Repeated framing-only measurements; payload is pre-encoded, network/encoding excluded. */
public final class BufferPushBenchmark {
  private static volatile long checksum;

  private static long sample(String mode, ByteBuffer encoded, int iterations) throws Exception {
    int size = encoded.remaining();
    long start = System.nanoTime();
    long sum = 0;
    for (int i = 0; i < iterations; i++) {
      if (mode.equals("two-copy")) {
        byte[] jni = new byte[size];
        encoded.duplicate().get(jni);
        byte[] body = new byte[size + 16];
        System.arraycopy(jni, 0, body, 16, size);
        sum += body[16] + body[body.length - 1];
      } else {
        ByteBuffer payload = encoded;
        if (mode.equals("one-copy")) {
          byte[] jni = new byte[size];
          encoded.duplicate().get(jni);
          payload = ByteBuffer.wrap(jni);
        }
        BufferPush push = new BufferPush(payload);
        push.setHeader(1, 2, i);
        ManagedBuffer request = push.newBuffer();
        ByteBuf netty = (ByteBuf) request.convertToNetty();
        sum += netty.getByte(16) + netty.getByte(size + 15);
        netty.release();
        request.release();
        push.succeed();
        push.releaseWork();
      }
    }
    checksum = sum;
    return System.nanoTime() - start;
  }

  public static void main(String[] args) throws Exception {
    com.sun.management.ThreadMXBean threads =
        (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    threads.setThreadAllocatedMemoryEnabled(true);
    long tid = Thread.currentThread().getId();
    for (int size : new int[] {4096, 65536, 1048576, 8388608}) {
      ByteBuffer encoded = ByteBuffer.allocateDirect(size);
      for (int i = 0; i < size; i++) {
        encoded.put(i, (byte) (i * 31));
      }
      int iterations = Math.max(16, (64 * 1024 * 1024) / size);
      for (String mode : new String[] {"two-copy", "one-copy", "direct"}) {
        for (int warm = 0; warm < 5; warm++) {
          sample(mode, encoded, iterations);
        }
      }
      long[][] ns = new long[3][9];
      long[][] allocation = new long[3][9];
      String[] modes = {"two-copy", "one-copy", "direct"};
      // Rotate execution order to reduce warm-up/thermal bias.
      for (int repeat = 0; repeat < 9; repeat++) {
        for (int ordinal = 0; ordinal < 3; ordinal++) {
          int index = (ordinal + repeat) % 3;
          long before = threads.getThreadAllocatedBytes(tid);
          ns[index][repeat] = sample(modes[index], encoded, iterations);
          allocation[index][repeat] = threads.getThreadAllocatedBytes(tid) - before;
        }
      }
      for (int index = 0; index < 3; index++) {
        Arrays.sort(ns[index]);
        Arrays.sort(allocation[index]);
        double median = ns[index][4] / (double) iterations;
        System.out.printf(
            java.util.Locale.ROOT,
            "{\"mode\":\"%s\",\"frame_bytes\":%d,\"iterations\":%d,\"repeats\":9,"
                + "\"median_ns_per_frame\":%.1f,\"min_ns_per_frame\":%.1f,\"max_ns_per_frame\":%.1f,"
                + "\"heap_allocated_bytes_per_frame\":%.1f}%n",
            modes[index],
            size,
            iterations,
            median,
            ns[index][0] / (double) iterations,
            ns[index][8] / (double) iterations,
            allocation[index][4] / (double) iterations);
      }
    }
  }
}
