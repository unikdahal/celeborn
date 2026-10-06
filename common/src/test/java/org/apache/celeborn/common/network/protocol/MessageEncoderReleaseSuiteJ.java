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

package org.apache.celeborn.common.network.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;

/**
 * Verifies that the message encoders release a converted body, and the reference on the message's
 * managed buffer, when encoding fails before an outbound message takes them over.
 */
public class MessageEncoderReleaseSuiteJ {

  /** Fails to allocate the frame header, after the body has already been converted. */
  private static ChannelHandlerContext contextFailingHeaderAllocation() {
    ByteBufAllocator failing = mock(ByteBufAllocator.class);
    when(failing.heapBuffer(anyInt()))
        .thenThrow(new IllegalStateException("mock header allocation failure"));
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.alloc()).thenReturn(failing);
    return ctx;
  }

  private static PushData pushData(ByteBuf body) {
    return new PushData((byte) 0, "shuffleKey", "partitionId", new NettyManagedBuffer(body));
  }

  @Test
  public void testMessageEncoderReleasesBodyWhenEncodingFailsAfterConversion() {
    ByteBuf body = Unpooled.directBuffer(32).writeZero(32);
    List<Object> out = new ArrayList<>();
    assertThrows(
        IllegalStateException.class,
        () ->
            MessageEncoder.INSTANCE.encode(contextFailingHeaderAllocation(), pushData(body), out));
    assertTrue(out.isEmpty());
    assertEquals(0, body.refCnt());
  }

  @Test
  public void testSslMessageEncoderReleasesBodyWhenEncodingFailsAfterConversion() {
    ByteBuf body = Unpooled.directBuffer(32).writeZero(32);
    List<Object> out = new ArrayList<>();
    assertThrows(
        IllegalStateException.class,
        () ->
            SslMessageEncoder.INSTANCE.encode(
                contextFailingHeaderAllocation(), pushData(body), out));
    assertTrue(out.isEmpty());
    assertEquals(0, body.refCnt());
  }

  @Test
  public void testSslMessageEncoderReleasesAnEmptyBodyThatIsNotSent() throws Exception {
    ByteBuf body = Unpooled.directBuffer(0);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    List<Object> out = new ArrayList<>();
    SslMessageEncoder.INSTANCE.encode(ctx, pushData(body), out);
    assertEquals(1, out.size());
    assertTrue(out.get(0) instanceof ByteBuf);
    ((ByteBuf) out.get(0)).release();
    assertEquals(0, body.refCnt());
  }
}
