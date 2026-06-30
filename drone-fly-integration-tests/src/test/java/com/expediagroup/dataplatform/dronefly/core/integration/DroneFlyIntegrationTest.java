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
package com.expediagroup.dataplatform.dronefly.core.integration;

import static java.util.Map.entry;

import static org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType.ADD_PARTITION;
import static org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType.CREATE_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.CLIENT_ID;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.TOPIC_NAME;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.DroneFlyIntegrationTestUtils.DATABASE;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.DroneFlyIntegrationTestUtils.TABLE;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.DroneFlyIntegrationTestUtils.TOPIC;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.DroneFlyIntegrationTestUtils.buildPartition;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.DroneFlyIntegrationTestUtils.awaitOffsetCommitted;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.DroneFlyIntegrationTestUtils.buildTable;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.DroneFlyIntegrationTestUtils.buildTableParameters;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.DummyListener.EVENT_COUNT_METRIC;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.SpringMetricsUtils.metric;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.SpringMetricsUtils.springMetricsIncrease;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import com.expediagroup.apiary.extensions.events.metastore.kafka.listener.KafkaMetaStoreEventListener;
import com.expediagroup.dataplatform.dronefly.app.DroneFly;
import com.expediagroup.dataplatform.dronefly.core.integration.support.AsyncRunnerConfig;
import com.expediagroup.dataplatform.dronefly.core.integration.support.DummyListener;
import com.google.common.collect.Lists;

@Import(AsyncRunnerConfig.class)
@SpringBootTest(
  classes = DroneFly.class,
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "apiary.bootstrap.servers=${spring.embedded.kafka.brokers}",
    "apiary.kafka.topic.name=" + TOPIC,
    "instance.name=test",
    "apiary.listener.list=com.expediagroup.dataplatform.dronefly.core.integration.support.DummyListener",
    "management.metrics.export.prometheus.enabled=true",
    "management.endpoints.web.exposure.include=health,info,prometheus,metrics"
  }
)
@EmbeddedKafka(count = 1, controlledShutdown = true, topics = {TOPIC}, partitions = 1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DroneFlyIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(DroneFlyIntegrationTest.class);
  // KafkaMessageReaderBuilder prefixes "apiary-kafka-metastore-receiver-" + instanceName
  private static final String CONSUMER_GROUP = "apiary-kafka-metastore-receiver-test";

  private HMSHandler hmsHandler = Mockito.mock(HMSHandler.class);

  private static Configuration CONF = new Configuration();

  private KafkaMetaStoreEventListener kafkaMetaStoreEventListener;

  @Autowired
  private TestRestTemplate restTemplate;

  @LocalManagementPort
  private int managementPort;

  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @BeforeAll
  void setUp() {
    log.info("Management URI: http://localhost:{}/actuator", managementPort);
    initKafkaListener();
  }

  @AfterEach
  public void reset() {
    DummyListener.reset();
  }

  @Test
  public void typical() {
    // verify that the consumer has read the events
    springMetricsIncrease(
      restTemplate,
      () -> {
        kafkaMetaStoreEventListener.onAddPartition(new AddPartitionEvent(buildTable(), buildPartition(), true, hmsHandler));
        kafkaMetaStoreEventListener.onCreateTable(new CreateTableEvent(buildTable(), true, hmsHandler));
      },
      entry(metric("drone.fly.events.received", "COUNT"), 2.0)
    );

    awaitOffsetCommitted(embeddedKafkaBroker, CONSUMER_GROUP, TOPIC, 0, 2L);
    assertThat(DummyListener.getNumEvents()).isEqualTo(2);

    double countBefore = EVENT_COUNT_METRIC.count();
    ListenerEvent receivedEventOne = DummyListener.get(0);
    ListenerEvent receivedEventTwo = DummyListener.get(1);

    assertEvent(receivedEventOne, ADD_PARTITION);
    assertEvent(receivedEventTwo, CREATE_TABLE);
    assertThat(EVENT_COUNT_METRIC.count()).isEqualTo(countBefore + 2.0);
  }

  private void assertEvent(ListenerEvent event, EventType eventType) {
    assertThat(event.getStatus()).isTrue();

    switch (eventType) {
      case ADD_PARTITION:
        assertThat(event).isInstanceOf(AddPartitionEvent.class);
        AddPartitionEvent addPartitionEvent = (AddPartitionEvent) event;
        assertThat(addPartitionEvent.getTable().getDbName()).isEqualTo(DATABASE);
        assertThat(addPartitionEvent.getTable().getTableName()).isEqualTo(TABLE);
        Iterator<Partition> iterator = addPartitionEvent.getPartitionIterator();
        List<Partition> partitions = new ArrayList<>();
        while (iterator.hasNext()) {
          partitions.add(iterator.next());
        }
        assertThat(partitions).isEqualTo(Lists.newArrayList(buildPartition()));
        assertThat(addPartitionEvent.getTable().getParameters()).isEqualTo(buildTableParameters());
        break;
      case CREATE_TABLE:
        assertThat(event).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) event;
        assertThat(createTableEvent.getTable().getDbName()).isEqualTo(DATABASE);
        assertThat(createTableEvent.getTable().getTableName()).isEqualTo(TABLE);
        break;
      default:
        fail(String
          .format("Received an event with type: {%s} that is different than ADD_PARTITION or CREATE_TABLE.",
            eventType));
        break;
    }
  }

  private void initKafkaListener() {
    CONF.set(BOOTSTRAP_SERVERS.key(), embeddedKafkaBroker.getBrokersAsString());
    CONF.set(CLIENT_ID.key(), "apiary-kafka-listener");
    CONF.set(TOPIC_NAME.key(), TOPIC);

    kafkaMetaStoreEventListener = new KafkaMetaStoreEventListener(CONF);
  }
}
