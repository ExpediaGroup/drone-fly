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
 * Compatibility shim for {@code DateColumnStatsData}. The Hive 4.x Thrift-generated class has both
 * {@code setBitVectors(byte[])} and {@code setBitVectors(ByteBuffer)}, and similarly for {@code
 * setHistogram}, which causes Jackson {@code InvalidDefinitionException}. This shim exposes only
 * the {@code byte[]} setters, resolving the conflict.
 */
public class DateColumnStatsData implements Serializable {

  private Date lowValue;
  private Date highValue;
  private long numNulls;
  private long numDVs;
  private byte[] bitVectors;
  private byte[] histogram;

  public DateColumnStatsData() {}

  public Date getLowValue() {
    return lowValue;
  }

  public void setLowValue(Date lowValue) {
    this.lowValue = lowValue;
  }

  public Date getHighValue() {
    return highValue;
  }

  public void setHighValue(Date highValue) {
    this.highValue = highValue;
  }

  public long getNumNulls() {
    return numNulls;
  }

  public void setNumNulls(long numNulls) {
    this.numNulls = numNulls;
  }

  public long getNumDVs() {
    return numDVs;
  }

  public void setNumDVs(long numDVs) {
    this.numDVs = numDVs;
  }

  public byte[] getBitVectors() {
    return bitVectors;
  }

  public void setBitVectors(byte[] bitVectors) {
    this.bitVectors = bitVectors;
  }

  public byte[] getHistogram() {
    return histogram;
  }

  public void setHistogram(byte[] histogram) {
    this.histogram = histogram;
  }
}
