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
package com.expediagroup.dataplatform.dronefly.core.integration.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import com.google.common.collect.Lists;

public class DroneFlyIntegrationTestUtils {

  public static final String TOPIC = "apiary-events";
  public static final String DATABASE = "database";
  public static final String TABLE = "table";

  public static Table buildTable() {
    return buildTable(TABLE);
  }

  public static Table buildTable(String tableName) {
    List<FieldSchema> partitions = Lists.newArrayList();
    partitions.add(new FieldSchema("a", "string", "comment"));
    partitions.add(new FieldSchema("b", "string", "comment"));
    partitions.add(new FieldSchema("c", "string", "comment"));
    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo("serde", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", new HashMap<>()));
    return new Table(tableName, DATABASE, "me", 1, 1, 1, sd, partitions, buildTableParameters(),
        "originalText", "expandedText", "tableType");
  }

  public static Partition buildPartition() {
    return buildPartition("partition");
  }

  public static Partition buildPartition(String partitionName) {
    List<String> values = Lists.newArrayList();
    values.add(partitionName + "1");
    values.add(partitionName + "2");
    StorageDescriptor sd = new StorageDescriptor();
    sd.setStoredAsSubDirectories(false);
    Partition partition = new Partition(values, DATABASE, TABLE, 1, 1, sd, buildTableParameters());
    partition.setWriteId(-1);
    partition.setIsStatsCompliant(false);
    return partition;
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

  public static void awaitOffsetCommitted(
      EmbeddedKafkaBroker broker, String consumerGroup, String topic, int partition, long expectedOffset) {
    try (AdminClient admin = AdminClient.create(
        Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()))) {
      await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
        Map<TopicPartition, OffsetAndMetadata> committed = admin
            .listConsumerGroupOffsets(consumerGroup)
            .partitionsToOffsetAndMetadata()
            .get(5, TimeUnit.SECONDS);
        OffsetAndMetadata offset = committed.get(new TopicPartition(topic, partition));
        assertThat(offset).isNotNull();
        assertThat(offset.offset()).isEqualTo(expectedOffset);
      });
    }
  }

}
