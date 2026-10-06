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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.junit.Test;

public class PushDataBodyBufferSuiteJ {

  @Test
  public void testEncoderOwnsReleaseOnceItClaimsTheBuffer() throws IOException {
    ByteBuf body = Unpooled.directBuffer(32).writeZero(32);
    PushDataBodyBuffer buffer = new PushDataBodyBuffer(body.retainedDuplicate());

    Object converted = buffer.convertToNetty();
    buffer.releaseIfNotEncoded();
    assertEquals("the encoder's claim must survive a late failure listener", 3, body.refCnt());
    // MessageWithHeader.deallocate releases both the converted body and the managed buffer.
    ReferenceCountUtil.release(converted);
    buffer.release();
    assertEquals(1, body.refCnt());
    body.release();
    assertEquals(0, body.refCnt());
  }

  @Test
  public void testUnencodedBufferIsReleasedOnceAndCannotBeEncoded() {
    ByteBuf body = Unpooled.directBuffer(32).writeZero(32);
    PushDataBodyBuffer buffer = new PushDataBodyBuffer(body.retainedDuplicate());

    buffer.releaseIfNotEncoded();
    buffer.releaseIfNotEncoded();
    assertEquals(1, body.refCnt());
    assertThrows(IOException.class, buffer::convertToNetty);
    // The encoder releases a body whose conversion failed; that must not release it twice.
    buffer.release();
    assertEquals(1, body.refCnt());
    body.release();
  }

  @Test
  public void testReleaseBeforeEncodingClaimsTheReferenceOnce() {
    ByteBuf body = Unpooled.directBuffer(32).writeZero(32);
    PushDataBodyBuffer buffer = new PushDataBodyBuffer(body.retainedDuplicate());

    buffer.release();
    buffer.releaseIfNotEncoded();
    buffer.release();
    assertEquals(1, body.refCnt());
    assertThrows(IOException.class, buffer::convertToNetty);
    body.release();
  }
}
