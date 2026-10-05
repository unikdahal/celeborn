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

import static org.junit.Assert.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import io.netty.buffer.ByteBuf;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;

public class BufferPushSuiteJ {
  @Test public void completionWaitsForEveryOwnerInEitherOrder() throws Exception {
    for (boolean terminalFirst : new boolean[]{false, true}) {
      BufferPush push = new BufferPush(ByteBuffer.allocateDirect(32));
      push.setHeader(2, 7, 9);
      ManagedBuffer first = push.newBuffer();
      ManagedBuffer retry = push.newBuffer();
      ByteBuf firstNetty = (ByteBuf) first.convertToNetty();
      CompletableFuture<Integer> done = push.completion().toCompletableFuture();
      push.releaseWork();
      if (terminalFirst) { push.succeed(); }
      firstNetty.release();
      first.release();
      assertFalse(done.isDone());
      if (!terminalFirst) { push.succeed(); }
      assertFalse(done.isDone());
      retry.release();
      assertEquals(48, (int) done.get());
      assertFalse(push.retainWork());
    }
  }
  @Test public void cancellationAndFailureDoNotRetireQueuedOrActiveWork() throws Exception {
    BufferPush push = new BufferPush(ByteBuffer.allocateDirect(32));
    assertTrue(push.retainWork()); // queued retry
    assertTrue(push.retainWork()); // running callback
    ManagedBuffer request = push.newBuffer();
    CompletableFuture<Integer> done = push.completion().toCompletableFuture();
    push.fail(new IllegalStateException("cancelled"));
    push.releaseWork();
    request.release();
    assertFalse(done.isDone());
    push.releaseWork();
    assertFalse(done.isDone());
    push.releaseWork();
    try { done.get(); fail(); }
    catch (ExecutionException expected) { assertEquals("cancelled", expected.getCause().getMessage()); }
    assertFalse(push.retainWork());
  }
  @Test public void nettyAliasesCanOutliveTheManagedReference() throws Exception {
    BufferPush push = new BufferPush(ByteBuffer.allocateDirect(32));
    ManagedBuffer request = push.newBuffer();
    ByteBuf netty = (ByteBuf) request.convertToNetty();
    ByteBuf alias = netty.retainedDuplicate();
    CompletableFuture<Integer> done = push.completion().toCompletableFuture();
    push.succeed(); push.releaseWork(); request.release(); netty.release();
    assertFalse(done.isDone());
    alias.release();
    assertEquals(48, (int) done.get());
  }
  @Test public void callerCannotCancelTheLifetimePromise() throws Exception {
    BufferPush push = new BufferPush(ByteBuffer.allocate(32));
    assertTrue(push.completion().toCompletableFuture().cancel(true));
    CompletableFuture<Integer> actual = push.completion().toCompletableFuture();
    assertFalse(actual.isDone());
    push.succeed();
    push.releaseWork();
    assertEquals(48, (int) actual.get());
  }
  @Test public void framingPreservesViewsAndAliasesPayloadWithoutCopy() throws Exception {
    for (boolean direct : new boolean[]{false, true}) {
      ByteBuffer source = direct ? ByteBuffer.allocateDirect(40) : ByteBuffer.allocate(40);
      source.position(3); source.limit(35);
      for (int i=3; i<35; i++) { source.put(i, (byte) i); }
      BufferPush push = new BufferPush(source.asReadOnlyBuffer());
      push.setHeader(2, 7, 9);
      ManagedBuffer request = push.newBuffer();
      ByteBuf bytes = (ByteBuf) request.convertToNetty();
      assertEquals(2, bytes.nioBufferCount());
      ByteBuffer head = bytes.nioBuffers()[0].order(ByteOrder.nativeOrder());
      assertEquals(2, head.getInt()); assertEquals(7, head.getInt());
      assertEquals(9, head.getInt()); assertEquals(32, head.getInt());
      assertEquals(3, source.position()); assertEquals(35, source.limit());
      // Deliberate mutation solely proves aliasing; the public API forbids caller mutation.
      source.put(3, (byte) 99);
      assertEquals(99, bytes.getByte(16));
      bytes.release(); request.release(); push.succeed(); push.releaseWork();
      assertEquals(48, (int) push.completion().toCompletableFuture().get());
    }
  }
}
