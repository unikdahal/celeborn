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
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;

import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;

/**
 * The body of one push data request, owning a single reference to its batch.
 *
 * <p>The message encoder normally takes over that reference and releases it once the write
 * finishes. Netty drops a message without encoding it if its write promise was cancelled or its
 * event loop rejected the write, so the reference would never be released. Whoever claims the
 * buffer first, the encoder or {@link #releaseIfNotEncoded()}, therefore owns its release.
 */
final class PushDataBodyBuffer extends NettyManagedBuffer {
  private static final int PENDING = 0;
  private static final int ENCODED = 1;
  private static final int RELEASED = 2;

  private final AtomicInteger state = new AtomicInteger(PENDING);

  PushDataBodyBuffer(ByteBuf buf) {
    super(buf);
  }

  @Override
  public Object convertToNetty() throws IOException {
    claimForEncoding();
    return super.convertToNetty();
  }

  @Override
  public Object convertToNettyForSsl() throws IOException {
    claimForEncoding();
    return super.convertToNettyForSsl();
  }

  @Override
  public ManagedBuffer release() {
    // The encoder releases a body it failed to convert; it was already released in that case.
    if (state.get() != RELEASED) {
      super.release();
    }
    return this;
  }

  /** Releases the reference if the write failed before the encoder took it over. */
  void releaseIfNotEncoded() {
    if (state.compareAndSet(PENDING, RELEASED)) {
      super.release();
    }
  }

  private void claimForEncoding() throws IOException {
    if (!state.compareAndSet(PENDING, ENCODED)) {
      throw new IOException("Push data body was released before it was encoded");
    }
  }
}
