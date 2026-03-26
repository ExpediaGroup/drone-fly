/**
 * Copyright (C) 2020-2026 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.api;

import java.io.Serializable;

/**
 * Compatibility shim for {@code BooleanColumnStatsData}. The Hive 4.x Thrift-generated class has
 * both {@code setBitVectors(byte[])} and {@code setBitVectors(ByteBuffer)}, which causes Jackson to
 * throw {@code InvalidDefinitionException: Conflicting setter definitions}. This shim exposes only
 * the {@code byte[]} setter, resolving the conflict.
 */
public class BooleanColumnStatsData implements Serializable {

  private long numTrues;
  private long numFalses;
  private long numNulls;
  private byte[] bitVectors;

  public BooleanColumnStatsData() {}

  public long getNumTrues() {
    return numTrues;
  }

  public void setNumTrues(long numTrues) {
    this.numTrues = numTrues;
  }

  public long getNumFalses() {
    return numFalses;
  }

  public void setNumFalses(long numFalses) {
    this.numFalses = numFalses;
  }

  public long getNumNulls() {
    return numNulls;
  }

  public void setNumNulls(long numNulls) {
    this.numNulls = numNulls;
  }

  public byte[] getBitVectors() {
    return bitVectors;
  }

  public void setBitVectors(byte[] bitVectors) {
    this.bitVectors = bitVectors;
  }
}
