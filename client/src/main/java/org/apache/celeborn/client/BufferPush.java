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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;

/** Completes only when a logical push and every work/transport owner have retired. */
final class BufferPush implements PushDataBody {
  private final ByteBuffer payload;
  private final byte[] header = new byte[16];
  private final CompletableFuture<Integer> completion = new CompletableFuture<>();
  private int references = 1; // the submitting invocation
  private boolean terminal;
  private Throwable failure;
  private int result;
  private int batchId;

  BufferPush(ByteBuffer payload) { this.payload = payload.slice().asReadOnlyBuffer(); }
  synchronized void setHeader(int mapId, int attemptId, int batchId) {
    this.batchId = batchId;
    ByteBuffer.wrap(header).order(ByteOrder.nativeOrder())
        .putInt(mapId).putInt(attemptId).putInt(batchId).putInt(payload.remaining());
  }
  int batchId() { return batchId; }
  public int length() { return header.length + payload.remaining(); }
  // A dependent stage isolates the internal lifetime promise from caller cancellation.
  CompletionStage<Integer> completion() { return completion.thenApply(size -> size); }
  public synchronized boolean retainWork() {
    if (terminal) { return false; }
    references++;
    return true;
  }
  public synchronized boolean isActive() { return !terminal; }
  public void releaseWork() {
    synchronized (this) {
      if (--references < 0) { throw new IllegalStateException("Unbalanced buffer push ownership"); }
    }
    completeIfRetired();
  }
  public void succeed() { succeed(length()); }
  void succeed(int size) { finish(size, null); }
  public void fail(Throwable failure) { finish(0, failure); }
  private void finish(int size, Throwable cause) {
    synchronized (this) {
      if (terminal) { return; }
      terminal = true;
      result = size;
      failure = cause;
    }
    completeIfRetired();
  }
  private void completeIfRetired() {
    int size;
    Throwable cause;
    synchronized (this) {
      if (!terminal || references != 0) { return; }
      size = result;
      cause = failure;
    }
    // Never run a caller's continuations under the ownership lock.
    if (cause == null) { completion.complete(size); }
    else { completion.completeExceptionally(cause); }
  }
  public ManagedBuffer newBuffer() {
    synchronized (this) {
      // A submitting invocation or retry already owns work; cancellation may race it.
      if (references <= 0) { throw new IllegalStateException("Buffer push is retired"); }
      references++;
    }
    try {
      CompositeByteBuf composite = Unpooled.compositeBuffer(2);
      composite.addComponent(true, Unpooled.wrappedBuffer(header));
      composite.addComponent(true, Unpooled.wrappedBuffer(payload.duplicate()));
      return new OwnedBuffer(new NettyManagedBuffer(composite));
    } catch (Throwable failure) {
      releaseWork();
      throw failure;
    }
  }
  private final class OwnedBuffer extends ManagedBuffer {
    private final NettyManagedBuffer delegate;
    OwnedBuffer(NettyManagedBuffer delegate) { this.delegate = delegate; }
    public long size() { return delegate.size(); }
    public ByteBuffer nioByteBuffer() throws IOException { return delegate.nioByteBuffer(); }
    public InputStream createInputStream() throws IOException { return delegate.createInputStream(); }
    public ManagedBuffer retain() {
      synchronized (BufferPush.this) { references++; }
      delegate.retain();
      return this;
    }
    public ManagedBuffer release() {
      delegate.release();
      releaseWork();
      return this;
    }
    public Object convertToNetty() throws IOException { return delegate.convertToNetty(); }
    public Object convertToNettyForSsl() throws IOException { return delegate.convertToNettyForSsl(); }
  }
}
