/**
 * Copyright (C) 2020 Expedia, Inc.
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
package com.expediagroup.dataplatform.dronefly.core.integration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.collect.Lists;

public class DroneFlyIntegrationTestUtils {

  static final String TOPIC = "apiary-events";
  static final String DATABASE = "database";
  static final String TABLE = "table";

  public static Table buildTable() {
    return buildTable(null);
  }

  public static Table buildTable(String tableName) {
    List<FieldSchema> partitions = Lists.newArrayList();
    partitions.add(new FieldSchema("a", "string", "comment"));
    partitions.add(new FieldSchema("b", "string", "comment"));
    partitions.add(new FieldSchema("c", "string", "comment"));
    return new Table(tableName == null ? TABLE : tableName, DATABASE, "me", 1, 1, 1, new StorageDescriptor(),
        partitions, buildTableParameters(), "originalText", "expandedText", "tableType");
  }

  public static Partition buildPartition() {
    return buildPartition(null);
  }

  public static Partition buildPartition(String partitionName) {
    List<String> values = Lists.newArrayList();
    values.add(partitionName + "1");
    values.add(partitionName + "2");
    StorageDescriptor sd = new StorageDescriptor();
    sd.setStoredAsSubDirectories(false);
    return new Partition(values, DATABASE, TABLE, 1, 1, sd, buildTableParameters());
  }

  public static Map<String, String> buildTableParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("key1", "value1");
    parameters.put("key2", "value2");
    return parameters;
  }

  public static String buildQualifiedTableName() {
    return DATABASE + "." + TABLE;
  }

}
