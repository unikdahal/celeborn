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

package org.apache.celeborn.common.meta;

import java.io.Serializable;
import java.util.Objects;

/** A monotonically fenced ownership lease for one logical Celeborn application namespace. */
public final class ApplicationLease implements Serializable {
  private final long epoch;
  private final String ownerId;
  private final long expiresAtMs;

  public ApplicationLease(long epoch, String ownerId, long expiresAtMs) {
    if (epoch <= 0) {
      throw new IllegalArgumentException("Application lease epoch must be positive");
    }
    if (ownerId == null || ownerId.isEmpty()) {
      throw new IllegalArgumentException("Application lease owner must be non-empty");
    }
    if (expiresAtMs <= 0) {
      throw new IllegalArgumentException("Application lease expiry must be positive");
    }
    this.epoch = epoch;
    this.ownerId = ownerId;
    this.expiresAtMs = expiresAtMs;
  }

  public long epoch() {
    return epoch;
  }

  public String ownerId() {
    return ownerId;
  }

  public long expiresAtMs() {
    return expiresAtMs;
  }

  public boolean isValid(long suppliedEpoch, String suppliedOwnerId, long nowMs) {
    return epoch == suppliedEpoch && ownerId.equals(suppliedOwnerId) && nowMs < expiresAtMs;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ApplicationLease)) {
      return false;
    }
    ApplicationLease that = (ApplicationLease) other;
    return epoch == that.epoch && expiresAtMs == that.expiresAtMs && ownerId.equals(that.ownerId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(epoch, ownerId, expiresAtMs);
  }

  @Override
  public String toString() {
    return "ApplicationLease(epoch="
        + epoch
        + ", ownerId="
        + ownerId
        + ", expiresAtMs="
        + expiresAtMs
        + ")";
  }
}
