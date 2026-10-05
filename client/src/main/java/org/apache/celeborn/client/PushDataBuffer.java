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

import io.netty.buffer.ByteBuf;

import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;

/** Owns a request body until the encoder takes it, including rejected/closed-channel writes. */
final class PushDataBuffer extends NettyManagedBuffer {
  private boolean encoded;
  private int managedReferences = 1;

  PushDataBuffer(ByteBuf buffer) { super(buffer); }

  @Override
  public synchronized Object convertToNetty() throws IOException {
    checkLive();
    Object result = super.convertToNetty();
    encoded = true;
    return result;
  }

  @Override
  public synchronized Object convertToNettyForSsl() throws IOException {
    checkLive();
    Object result = super.convertToNettyForSsl();
    encoded = true;
    return result;
  }

  @Override
  public synchronized ManagedBuffer retain() {
    checkLive();
    super.retain();
    managedReferences++;
    return this;
  }

  @Override
  public synchronized ManagedBuffer release() {
    // A rejected write and the encoder's error cleanup can race. Retire each managed
    // reference once; converted/retained ByteBuf aliases have independent Netty references.
    if (managedReferences > 0) {
      managedReferences--;
      super.release();
    }
    return this;
  }

  synchronized void releaseIfUnencoded() {
    if (!encoded) { release(); }
  }

  private void checkLive() {
    if (managedReferences == 0) { throw new IllegalStateException("Push body already released"); }
  }
}
