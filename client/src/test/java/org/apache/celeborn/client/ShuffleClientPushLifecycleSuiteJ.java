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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.client.TransportResponseHandler;
import org.apache.celeborn.common.network.protocol.MessageEncoder;
import org.apache.celeborn.common.network.protocol.PushData;
import org.apache.celeborn.common.network.protocol.RpcFailure;
import org.apache.celeborn.common.network.protocol.RpcResponse;
import org.apache.celeborn.common.network.protocol.SerdeVersion;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.CompressionCodec;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.ControlMessages.RegisterShuffleResponse$;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.rpc.RpcEndpointRef;

/**
 * Verifies that every push data batch body is released exactly once on each terminal outcome, and
 * only after both the push lifecycle and every transport write have finished with it. Uses a real
 * {@link TransportClient} and {@link MessageEncoder} on an {@link EmbeddedChannel}, and tracks the
 * reference count of each batch body through {@link ShuffleClientImpl#newBatchBody}.
 */
public class ShuffleClientPushLifecycleSuiteJ {
  private static final int SHUFFLE_ID = 1;
  private static final int MAP_ID = 2;
  private static final int ATTEMPT_ID = 3;
  private static final int PARTITION_ID = 0;
  private static final PartitionLocation LOCATION =
      new PartitionLocation(
          PARTITION_ID, 1, "localhost", 1234, 1235, 1236, 1237, PartitionLocation.Mode.PRIMARY);
  private static final byte[] DATA = new byte[256];

  private ShuffleClientImpl shuffleClient;
  private EmbeddedChannel channel;
  private TransportResponseHandler handler;
  private final List<Long> requestIds = new CopyOnWriteArrayList<>();
  private final List<ByteBuf> bodies = new CopyOnWriteArrayList<>();
  private boolean failHeaderAllocation;
  private TransportClientFactory factory;

  /** Drops every write before it is encoded, as Netty does for cancelled writes. */
  private static final class DroppingHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
      ReferenceCountUtil.release(msg);
      promise.setFailure(new IOException("write dropped before encoding"));
    }
  }

  @After
  public void tearDown() {
    if (shuffleClient != null) {
      shuffleClient.shutdown();
    }
    if (channel != null) {
      channel.finishAndReleaseAll();
    }
  }

  private void setup(int maxReviveTimes, boolean dropWrites) throws Exception {
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.SHUFFLE_COMPRESSION_CODEC().key(), CompressionCodec.NONE.name());
    conf.set(CelebornConf.CLIENT_PUSH_RETRY_THREADS().key(), "1");
    conf.set(CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES().key(), String.valueOf(maxReviveTimes));
    // The in-flight wait defaults to a multiple of the revive count, which is 0 in some tests.
    conf.set(CelebornConf.CLIENT_PUSH_LIMIT_IN_FLIGHT_TIMEOUT().key(), "10s");
    conf.set(CelebornConf.CLIENT_PUSH_REVIVE_INTERVAL().key(), "10ms");
    conf.set(CelebornConf.CLIENT_RPC_REQUEST_PARTITION_LOCATION_ASK_TIMEOUT().key(), "2s");
    shuffleClient =
        new ShuffleClientImpl("lifecycle-app", conf, new UserIdentifier("mock", "mock")) {
          @Override
          protected ByteBuf newBatchBody(byte[] framedBatch) {
            ByteBuf body = Unpooled.directBuffer(framedBatch.length).writeBytes(framedBatch);
            bodies.add(body);
            return body;
          }
        };

    RpcEndpointRef endpointRef = mock(RpcEndpointRef.class);
    // Registration succeeds; any other request, such as a revive, gets an unusable response and
    // therefore fails.
    when(endpointRef.askSync(any(), any(), any()))
        .thenAnswer(
            t ->
                RegisterShuffleResponse$.MODULE$.apply(
                    StatusCode.SUCCESS, new PartitionLocation[] {LOCATION}, SerdeVersion.V1));
    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            t ->
                RegisterShuffleResponse$.MODULE$.apply(
                    StatusCode.SUCCESS, new PartitionLocation[] {LOCATION}, SerdeVersion.V1));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    channel =
        dropWrites
            ? new EmbeddedChannel(MessageEncoder.INSTANCE, new DroppingHandler())
            : new EmbeddedChannel(MessageEncoder.INSTANCE);
    if (failHeaderAllocation) {
      // The encoder converts the body, then fails to allocate the frame header.
      ByteBufAllocator failing = mock(ByteBufAllocator.class);
      when(failing.heapBuffer(anyInt()))
          .thenThrow(new IllegalStateException("mock header allocation failure"));
      channel.config().setAllocator(failing);
    }
    handler = new TransportResponseHandler(new TransportConf("data", conf), channel);
    TransportClient client =
        new TransportClient(channel, handler) {
          @Override
          public io.netty.channel.ChannelFuture pushData(
              PushData pushData, long timeout, RpcResponseCallback callback) {
            io.netty.channel.ChannelFuture future = super.pushData(pushData, timeout, callback);
            requestIds.add(pushData.requestId);
            return future;
          }
        };
    factory = mock(TransportClientFactory.class);
    when(factory.createClient(anyString(), anyInt(), anyInt())).thenReturn(client);
    shuffleClient.dataClientFactory = factory;
  }

  private int push() throws IOException {
    return shuffleClient.pushData(
        SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID, DATA, 0, DATA.length, 1, 1);
  }

  /** Completes the oldest queued write, as the socket would once it has sent the bytes. */
  private void completeWrite() {
    Object message = channel.readOutbound();
    assertTrue("expected an encoded push data request", message instanceof FileRegion);
    ReferenceCountUtil.release(message);
  }

  private void respond(int request, byte... status) throws Exception {
    handler.handle(
        new RpcResponse(requestIds.get(request), new NioManagedBuffer(ByteBuffer.wrap(status))));
  }

  private void fail(int request) throws Exception {
    handler.handle(new RpcFailure(requestIds.get(request), "mock push failure"));
  }

  private static void await(String what, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean()) {
      assertTrue("timed out waiting until " + what, System.nanoTime() < deadline);
      Thread.sleep(5);
    }
  }

  private ByteBuf onlyBody() {
    assertEquals(1, bodies.size());
    return bodies.get(0);
  }

  @Test
  public void testBodyIsReleasedOnlyAfterBothResponseAndWrite() throws Exception {
    setup(0, false);
    push();
    ByteBuf body = onlyBody();
    respond(0);
    // A queued write holds two references: its own, and the one the encoder converted.
    assertEquals("the queued write still holds the body", 2, body.refCnt());
    completeWrite();
    assertEquals(0, body.refCnt());

    push();
    ByteBuf second = bodies.get(1);
    completeWrite();
    assertEquals("the push lifecycle still holds the body", 1, second.refCnt());
    respond(1);
    assertEquals(0, second.refCnt());
  }

  @Test
  public void testWriteDroppedBeforeEncodingDoesNotLeakTheBody() throws Exception {
    setup(0, true);
    push();
    channel.runPendingTasks();
    assertEquals(0, onlyBody().refCnt());
    // The batch failed permanently, so the next push reports it instead of waiting for it.
    assertThrows(IOException.class, this::push);
  }

  @Test
  public void testExhaustedRetriesReleaseTheBody() throws Exception {
    setup(0, false);
    push();
    ByteBuf body = onlyBody();
    fail(0);
    assertEquals("the queued write still holds the body", 2, body.refCnt());
    completeWrite();
    assertEquals(0, body.refCnt());
    assertThrows(IOException.class, this::push);
  }

  @Test
  public void testFailedReviveReleasesTheBodyOnce() throws Exception {
    setup(1, false);
    push();
    ByteBuf body = onlyBody();
    completeWrite();
    // No newer location exists and the revive request fails, which exhausts the single retry.
    fail(0);
    await("the failed revive ends the batch", () -> body.refCnt() == 0);
    assertEquals(1, requestIds.size());
    assertThrows(IOException.class, this::push);
  }

  @Test
  public void testRetrySucceedsAfterReviveAndReleasesTheBodyOnce() throws Exception {
    setup(1, false);
    push();
    ByteBuf body = onlyBody();
    completeWrite();
    // A newer location for the partition makes the revive succeed without a request.
    PartitionLocation revived =
        new PartitionLocation(
            PARTITION_ID, 2, "localhost", 2234, 2235, 2236, 2237, PartitionLocation.Mode.PRIMARY);
    shuffleClient.reducePartitionMap.get(SHUFFLE_ID).put(PARTITION_ID, revived);
    fail(0);
    await("the batch is resent to the revived location", () -> requestIds.size() == 2);
    channel.runPendingTasks();
    assertEquals("the push lifecycle and the queued retry hold the body", 3, body.refCnt());
    completeWrite();
    assertEquals(1, body.refCnt());
    respond(1);
    assertEquals(0, body.refCnt());
    // The batch succeeded, so the attempt can continue.
    push();
    assertEquals(2, bodies.size());
  }

  @Test
  public void testMapperEndedDuringReviveReleasesTheBody() throws Exception {
    setup(1, false);
    push();
    ByteBuf body = onlyBody();
    completeWrite();
    shuffleClient
        .mapperEndMap
        .computeIfAbsent(SHUFFLE_ID, id -> ConcurrentHashMap.newKeySet())
        .add(MAP_ID);
    respond(0, StatusCode.HARD_SPLIT.getValue());
    await("the retry drops the batch of the ended mapper", () -> body.refCnt() == 0);
    assertEquals("nothing is resent for an ended mapper", 1, requestIds.size());
  }

  @Test
  public void testRejectedRetryFailsTheBatchAndReleasesTheBody() throws Exception {
    setup(1, false);
    push();
    ByteBuf body = onlyBody();
    completeWrite();
    shuffleClient.shutdown();
    // Before the fix, the rejected submission escaped from the response handler and the batch
    // never completed.
    respond(0, StatusCode.HARD_SPLIT.getValue());
    assertEquals(0, body.refCnt());
    IOException failure = assertThrows(IOException.class, this::push);
    boolean rejected = false;
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      rejected |= String.valueOf(cause.getMessage()).contains("retry was rejected");
    }
    assertTrue("expected the rejected retry to fail the batch: " + failure, rejected);
    shuffleClient = null;
  }

  @Test
  public void testEncodingFailureAfterConversionDoesNotLeakTheBody() throws Exception {
    failHeaderAllocation = true;
    setup(0, false);
    push();
    channel.runPendingTasks();
    assertEquals(0, onlyBody().refCnt());
    assertThrows(IOException.class, this::push);
  }

  @Test
  public void testRetryThatThrowsFailsTheBatchAndReleasesTheBodyOnce() throws Exception {
    setup(1, false);
    push();
    ByteBuf body = onlyBody();
    completeWrite();
    PartitionLocation revived =
        new PartitionLocation(
            PARTITION_ID, 2, "localhost", 2234, 2235, 2236, 2237, PartitionLocation.Mode.PRIMARY);
    shuffleClient.reducePartitionMap.get(SHUFFLE_ID).put(PARTITION_ID, revived);
    // The retry fails with an error rather than an exception, which nothing in the retry catches
    // and the retry pool's unobserved future would otherwise swallow.
    when(factory.createClient(anyString(), anyInt(), anyInt()))
        .thenThrow(new AssertionError("mock unexpected error"));
    fail(0);
    await("the failed retry ends the batch", () -> body.refCnt() == 0);
    assertEquals(1, requestIds.size());
    assertFailureCause(assertThrows(IOException.class, this::push), "retry failed");
  }

  @Test
  public void testLocationRemovedWhileRetryIsPendingReleasesTheBodyOnce() throws Exception {
    setup(1, false);
    push();
    ByteBuf body = onlyBody();
    completeWrite();
    PartitionLocation revived =
        new PartitionLocation(
            PARTITION_ID, 2, "localhost", 2234, 2235, 2236, 2237, PartitionLocation.Mode.PRIMARY);
    // The revive sees the new location, but by the time the retry looks it up, the shuffle's
    // locations are gone, as after a concurrent cleanup.
    shuffleClient.reducePartitionMap.put(
        SHUFFLE_ID,
        new ConcurrentHashMap<Integer, PartitionLocation>() {
          @Override
          public PartitionLocation get(Object key) {
            return Thread.currentThread().getName().startsWith("celeborn-retry-sender")
                ? null
                : revived;
          }
        });
    fail(0);
    await("the failed retry ends the batch", () -> body.refCnt() == 0);
    assertEquals(1, requestIds.size());
  }

  private static void assertFailureCause(Throwable failure, String expected) {
    boolean found = false;
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      found |= String.valueOf(cause.getMessage()).contains(expected);
    }
    assertTrue("expected a cause containing '" + expected + "': " + failure, found);
  }
}
