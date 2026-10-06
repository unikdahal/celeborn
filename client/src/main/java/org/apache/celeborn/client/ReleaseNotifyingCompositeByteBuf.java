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

import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A push data body that runs a callback once its last reference is released.
 *
 * <p>The client keeps one reference while a batch may still be sent or retried, and each transport
 * write holds another through a retained duplicate. The callback therefore runs only after both the
 * push lifecycle and every Netty write have finished with the components, which is when a caller
 * may reuse or free memory wrapped by them.
 */
final class ReleaseNotifyingCompositeByteBuf extends CompositeByteBuf {
  private static final Logger logger =
      LoggerFactory.getLogger(ReleaseNotifyingCompositeByteBuf.class);

  private final AtomicBoolean notified = new AtomicBoolean();
  private final Runnable releaseCallback;

  ReleaseNotifyingCompositeByteBuf(Runnable releaseCallback, ByteBuf... components) {
    super(UnpooledByteBufAllocator.DEFAULT, false, components.length, components);
    this.releaseCallback = releaseCallback;
  }

  @Override
  protected void deallocate() {
    try {
      super.deallocate();
    } finally {
      if (notified.compareAndSet(false, true)) {
        try {
          releaseCallback.run();
        } catch (Throwable t) {
          logger.error("Push data release callback failed.", t);
        }
      }
    }
  }
}
