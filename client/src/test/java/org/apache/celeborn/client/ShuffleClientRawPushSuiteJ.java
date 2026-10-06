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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Verifies the ownership contract of {@link ShuffleClient#pushRawData}: the payload is framed
 * without being copied, and the release callback runs exactly once, only after both the push
 * lifecycle and the transport write have released the payload.
 */
public class ShuffleClientRawPushSuiteJ {
  private static final int SHUFFLE_ID = 1;
  private static final int MAP_ID = 2;
  private static final int ATTEMPT_ID = 3;
  private static final int PARTITION_ID = 0;
  private static final int HEADER_SIZE = 16;

  private static final PartitionLocation LOCATION =
      new PartitionLocation(
          PARTITION_ID, 1, "localhost", 1234, 1235, 1236, 1237, PartitionLocation.Mode.PRIMARY);

  private ShuffleClientImpl shuffleClient;
  private EmbeddedChannel channel;
  private RecordingTransportClient transportClient;

  /** Records the request ID of each push so that tests can answer it. */
  private static final class RecordingTransportClient extends TransportClient {
    final List<Long> requestIds = new ArrayList<>();
    final TransportResponseHandler handler;

    RecordingTransportClient(EmbeddedChannel channel, TransportResponseHandler handler) {
      super(channel, handler);
      this.handler = handler;
    }

    @Override
    public io.netty.channel.ChannelFuture pushData(
        PushData pushData, long pushDataTimeout, RpcResponseCallback callback) {
      io.netty.channel.ChannelFuture future = super.pushData(pushData, pushDataTimeout, callback);
      requestIds.add(pushData.requestId);
      return future;
    }

    void respond(int index, byte... status) throws Exception {
      handler.handle(
          new RpcResponse(requestIds.get(index), new NioManagedBuffer(ByteBuffer.wrap(status))));
    }

    void fail(int index) throws Exception {
      handler.handle(new RpcFailure(requestIds.get(index), "mock push failure"));
    }
  }

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
    setup(maxReviveTimes, dropWrites, StatusCode.SUCCESS);
  }

  private void setup(int maxReviveTimes, boolean dropWrites, StatusCode registerStatus)
      throws Exception {
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.SHUFFLE_COMPRESSION_CODEC().key(), CompressionCodec.NONE.name());
    conf.set(CelebornConf.CLIENT_PUSH_RETRY_THREADS().key(), "1");
    conf.set(CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES().key(), String.valueOf(maxReviveTimes));
    // The in-flight wait defaults to a multiple of the revive count, which is 0 in some tests.
    conf.set(CelebornConf.CLIENT_PUSH_LIMIT_IN_FLIGHT_TIMEOUT().key(), "10s");
    conf.set(CelebornConf.CLIENT_REGISTER_SHUFFLE_MAX_RETRIES().key(), "1");
    conf.set("celeborn.client.push.maxReqsInFlight.perWorker", "1000");
    conf.set("celeborn.client.push.maxReqsInFlight.total", "1000");
    conf.set(CelebornConf.CLIENT_REGISTER_SHUFFLE_RETRY_WAIT().key(), "1ms");
    shuffleClient = new ShuffleClientImpl("raw-push-app", conf, new UserIdentifier("mock", "mock"));

    RpcEndpointRef endpointRef = mock(RpcEndpointRef.class);
    when(endpointRef.askSync(any(), any(), any()))
        .thenAnswer(
            t ->
                RegisterShuffleResponse$.MODULE$.apply(
                    registerStatus, new PartitionLocation[] {LOCATION}, SerdeVersion.V1));
    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            t ->
                RegisterShuffleResponse$.MODULE$.apply(
                    registerStatus, new PartitionLocation[] {LOCATION}, SerdeVersion.V1));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    channel =
        dropWrites
            ? new EmbeddedChannel(MessageEncoder.INSTANCE, new DroppingHandler())
            : new EmbeddedChannel(MessageEncoder.INSTANCE);
    TransportResponseHandler handler =
        new TransportResponseHandler(new TransportConf("data", conf), channel);
    transportClient = new RecordingTransportClient(channel, handler);
    TransportClientFactory factory = mock(TransportClientFactory.class);
    when(factory.createClient(anyString(), anyInt(), anyInt())).thenReturn(transportClient);
    shuffleClient.dataClientFactory = factory;
  }

  private static ByteBuffer directPayload(int length, int seed) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(length);
    for (int i = 0; i < length; i++) {
      buffer.put(i, (byte) (seed + i * 31));
    }
    return buffer;
  }

  private int push(ByteBuffer data, Runnable releaseCallback) throws IOException {
    return shuffleClient.pushRawData(
        SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID, data, 1, 1, releaseCallback);
  }

  /** Completes the oldest write, returning the bytes that the transport wrote for it. */
  private byte[] completeWrite() throws IOException {
    Object message = channel.readOutbound();
    assertTrue("expected an encoded push data request", message instanceof FileRegion);
    FileRegion region = (FileRegion) message;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      while (region.transferred() < region.count()) {
        region.transferTo(Channels.newChannel(out), region.transferred());
      }
    } finally {
      region.release();
    }
    return out.toByteArray();
  }

  /** Batch IDs of a map attempt start at 1. */
  private static byte[] expectedBody(int batchId, ByteBuffer payload) {
    ByteBuffer body = ByteBuffer.allocate(HEADER_SIZE + payload.remaining());
    body.order(ByteOrder.nativeOrder());
    body.putInt(MAP_ID).putInt(ATTEMPT_ID).putInt(batchId).putInt(payload.remaining());
    body.put(payload.duplicate());
    return body.array();
  }

  private static byte[] tail(byte[] bytes, int length) {
    byte[] tail = new byte[length];
    System.arraycopy(bytes, bytes.length - length, tail, 0, length);
    return tail;
  }

  @Test
  public void testFramesPayloadWithoutCopyAndReleasesAfterResponseAndWrite() throws Exception {
    setup(0, false);
    ByteBuffer data = directPayload(4096, 7);
    data.position(5);
    AtomicInteger releases = new AtomicInteger();

    int pushed = push(data, releases::incrementAndGet);

    assertEquals(HEADER_SIZE + 4091, pushed);
    assertEquals("payload must not be released while it is queued", 0, releases.get());
    transportClient.respond(0);
    assertEquals("payload must not be released while the write holds it", 0, releases.get());
    byte[] written = completeWrite();
    assertEquals(1, releases.get());
    assertArrayEquals(expectedBody(1, data), tail(written, pushed));
    assertEquals("the caller's position must not move", 5, data.position());
  }

  @Test
  public void testReleasesAfterResponseWhenWriteCompletesFirst() throws Exception {
    setup(0, false);
    AtomicInteger releases = new AtomicInteger();

    push(directPayload(100, 1), releases::incrementAndGet);
    completeWrite();
    assertEquals("payload must not be released before the push is acknowledged", 0, releases.get());
    transportClient.respond(0);
    assertEquals(1, releases.get());
  }

  @Test
  public void testReleasesAfterPermanentFailure() throws Exception {
    setup(0, false);
    AtomicInteger releases = new AtomicInteger();

    push(directPayload(100, 1), releases::incrementAndGet);
    transportClient.fail(0);
    assertEquals(0, releases.get());
    completeWrite();
    assertEquals(1, releases.get());

    // The failure is reported by the next push, which still releases its own payload.
    AtomicInteger nextReleases = new AtomicInteger();
    assertThrows(
        IOException.class, () -> push(directPayload(10, 2), nextReleases::incrementAndGet));
    assertEquals(1, nextReleases.get());
  }

  @Test
  public void testReleasesWriteDroppedBeforeEncoding() throws Exception {
    setup(0, true);
    AtomicInteger releases = new AtomicInteger();

    push(directPayload(100, 1), releases::incrementAndGet);
    channel.runPendingTasks();
    assertEquals(1, releases.get());
  }

  @Test
  public void testReleasesWhenMapperAlreadyEnded() throws Exception {
    setup(0, false);
    AtomicInteger first = new AtomicInteger();
    push(directPayload(100, 1), first::incrementAndGet);
    transportClient.respond(0, StatusCode.MAP_ENDED.getValue());
    completeWrite();
    assertEquals(1, first.get());

    AtomicInteger second = new AtomicInteger();
    assertEquals(0, push(directPayload(100, 2), second::incrementAndGet));
    assertEquals(1, second.get());
  }

  @Test
  public void testReleasesWhenRegistrationFails() throws Exception {
    setup(0, false, StatusCode.SLOT_NOT_AVAILABLE);
    AtomicInteger releases = new AtomicInteger();

    assertThrows(IOException.class, () -> push(directPayload(100, 1), releases::incrementAndGet));
    assertEquals(1, releases.get());
  }

  @Test
  public void testReleasesWhenRetryIsRejected() throws Exception {
    setup(1, false);
    AtomicInteger releases = new AtomicInteger();

    push(directPayload(100, 1), releases::incrementAndGet);
    completeWrite();
    shuffleClient.shutdown();
    transportClient.respond(0, StatusCode.HARD_SPLIT.getValue());
    assertEquals(1, releases.get());
    shuffleClient = null;
  }

  @Test
  public void testEncryptionReleasesCallerBufferBeforeReturning() throws Exception {
    setup(0, false);
    shuffleClient.setupCryptoHandler(
        Optional.of(
            new org.apache.celeborn.client.security.CryptoHandler() {
              @Override
              public byte[] encrypt(byte[] input, int offset, int length) {
                byte[] out = new byte[length];
                for (int i = 0; i < length; i++) {
                  out[i] = (byte) ~input[offset + i];
                }
                return out;
              }

              @Override
              public byte[] decrypt(byte[] input, int offset, int length) {
                return encrypt(input, offset, length);
              }
            }));
    ByteBuffer data = directPayload(64, 3);
    AtomicInteger releases = new AtomicInteger();

    int pushed = push(data, releases::incrementAndGet);
    assertEquals(1, releases.get());

    ByteBuffer encrypted = ByteBuffer.allocate(64);
    for (int i = 0; i < 64; i++) {
      encrypted.put(i, (byte) ~data.get(i));
    }
    assertArrayEquals(expectedBody(1, encrypted), tail(completeWrite(), pushed));
    transportClient.respond(0);
    assertEquals(1, releases.get());
  }

  @Test
  public void testEveryPayloadIsReleasedExactlyOnceInAnyCompletionOrder() throws Exception {
    setup(0, false);
    Random random = new Random(42);
    int batches = 20;
    int pushesPerBatch = 50;
    for (int batch = 0; batch < batches; batch++) {
      List<AtomicInteger> releases = new ArrayList<>();
      int firstRequest = transportClient.requestIds.size();
      for (int i = 0; i < pushesPerBatch; i++) {
        AtomicInteger counter = new AtomicInteger();
        releases.add(counter);
        push(directPayload(1 + random.nextInt(2048), i), counter::incrementAndGet);
      }
      List<Runnable> completions = new ArrayList<>();
      for (int i = 0; i < pushesPerBatch; i++) {
        int request = firstRequest + i;
        completions.add(
            () -> {
              try {
                transportClient.respond(request);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
        completions.add(
            () -> {
              try {
                completeWrite();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
      }
      Collections.shuffle(completions, random);
      completions.forEach(Runnable::run);
      for (AtomicInteger counter : releases) {
        assertEquals(1, counter.get());
      }
    }
  }
}
