/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.celeborn.common.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Validation shared by every recovery-task-commit ingress and metadata restore path. */
public final class RecoveryTaskCommitUtils {
  // Identity fields are copied into RPCs, the Raft log, snapshot keys, and every stored record.
  // A hard protocol bound avoids configuration skew between clients and metadata replicas.
  public static final int MAX_IDENTITY_UTF8_BYTES = 1024;

  private RecoveryTaskCommitUtils() {}

  public static void validateIdentity(
      String appId, String recoveryId, String writeId, int partitionId) {
    validateIdentityPart("application id", appId);
    validateIdentityPart("recovery id", recoveryId);
    validateIdentityPart("write id", writeId);
    if (partitionId < -1) {
      throw new IllegalArgumentException(
          "Recovery task commit partition id must be -1 or non-negative");
    }
  }

  public static void validatePayload(byte[] payload, byte[] sha256, long maxPayloadBytes) {
    if (payload == null || payload.length == 0 || payload.length > maxPayloadBytes) {
      throw new IllegalArgumentException(
          "Recovery task commit payload must contain between 1 and "
              + maxPayloadBytes
              + " bytes");
    }
    if (sha256 == null || sha256.length != 32) {
      throw new IllegalArgumentException("Recovery task commit requires a 32-byte SHA-256 digest");
    }
    final byte[] actual;
    try {
      actual = MessageDigest.getInstance("SHA-256").digest(payload);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("JVM does not provide SHA-256", e);
    }
    if (!MessageDigest.isEqual(actual, sha256)) {
      throw new IllegalArgumentException("Recovery task commit payload SHA-256 mismatch");
    }
  }

  private static void validateIdentityPart(String name, String value) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("Recovery task commit " + name + " must be non-empty");
    }
    final ByteBuffer encoded;
    try {
      encoded =
          StandardCharsets.UTF_8
              .newEncoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .encode(CharBuffer.wrap(value));
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException(
          "Recovery task commit " + name + " is not valid Unicode", e);
    }
    if (encoded.remaining() > MAX_IDENTITY_UTF8_BYTES) {
      throw new IllegalArgumentException(
          "Recovery task commit "
              + name
              + " exceeds "
              + MAX_IDENTITY_UTF8_BYTES
              + " UTF-8 bytes");
    }
  }
}
