/**
 * Copyright (C) 2020-2025 Expedia, Inc.
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

import static org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType.ADD_PARTITION;
import static org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType.CREATE_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;

import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.CLIENT_ID;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.TOPIC_NAME;
import static com.expediagroup.dataplatform.dronefly.core.integration.DroneFlyIntegrationTestUtils.DATABASE;
import static com.expediagroup.dataplatform.dronefly.core.integration.DroneFlyIntegrationTestUtils.TABLE;
import static com.expediagroup.dataplatform.dronefly.core.integration.DroneFlyIntegrationTestUtils.TOPIC;
import static com.expediagroup.dataplatform.dronefly.core.integration.DroneFlyIntegrationTestUtils.buildPartition;
import static com.expediagroup.dataplatform.dronefly.core.integration.DroneFlyIntegrationTestUtils.buildTable;
import static com.expediagroup.dataplatform.dronefly.core.integration.DroneFlyIntegrationTestUtils.buildTableParameters;
import static com.expediagroup.dataplatform.dronefly.core.integration.DummyListener.EVENT_COUNT_METRIC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.collect.Lists;

import com.expediagroup.apiary.extensions.events.metastore.kafka.listener.KafkaMetaStoreEventListener;
import com.expediagroup.dataplatform.dronefly.app.DroneFly;

@EmbeddedKafka(count = 1, controlledShutdown = true, topics = { TOPIC }, partitions = 1)
@ExtendWith(SpringExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DroneFlyIntegrationTest {

  private @Mock HMSHandler hmsHandler;

  private final ExecutorService executorService = Executors.newFixedThreadPool(1);
  private static Configuration CONF = new Configuration();

  private KafkaMetaStoreEventListener kafkaMetaStoreEventListener;

  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  private BlockingQueue<ConsumerRecord<String, String>> records;

  private KafkaMessageListenerContainer<String, String> container;

  @BeforeAll
  void setUp() throws InterruptedException {
    /**
     * The function initEmbeddedKafka() is required so that EmbeddedKafka waits for the consumer group assignment to
     * complete.
     * https://stackoverflow.com/questions/47312373/embeddedkafka-sending-messages-to-consumer-after-delay-in-subsequent-test
     */
    initEmbeddedKafka();
    System.setProperty("instance.name", "test");
    System.setProperty("apiary.bootstrap.servers", embeddedKafkaBroker.getBrokersAsString());
    System.setProperty("apiary.kafka.topic.name", TOPIC);
    System.setProperty("apiary.listener.list", "com.expediagroup.dataplatform.dronefly.core.integration.DummyListener");
    initKafkaListener();

    executorService.execute(() -> DroneFly.main(new String[] {}));
    await().atMost(Duration.FIVE_MINUTES).until(DroneFly::isRunning);
  }

  @AfterEach
  public void reset() {
    DummyListener.reset();
  }

  @AfterAll
  public void stop() throws InterruptedException {
    DroneFly.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void typical() {
    AddPartitionEvent addPartitionEvent = new AddPartitionEvent(buildTable(), buildPartition(), true, hmsHandler);
    kafkaMetaStoreEventListener.onAddPartition(addPartitionEvent);

    CreateTableEvent createTableEvent = new CreateTableEvent(buildTable(), true, hmsHandler);
    kafkaMetaStoreEventListener.onCreateTable(createTableEvent);

    await().atMost(5, TimeUnit.SECONDS).until(() -> DummyListener.getNumEvents() > 1);

    assertThat(DummyListener.getNumEvents()).isEqualTo(2);

    ListenerEvent receivedEventOne = DummyListener.get(0);
    ListenerEvent receivedEventTwo = DummyListener.get(1);

    assertEvent(receivedEventOne, ADD_PARTITION);
    assertEvent(receivedEventTwo, CREATE_TABLE);
    assertThat(EVENT_COUNT_METRIC.count()).isEqualTo(2.0);
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

  private void initEmbeddedKafka() {
    Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("consumer", "false", embeddedKafkaBroker));
    DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs,
        new StringDeserializer(), new StringDeserializer());
    ContainerProperties containerProperties = new ContainerProperties(TOPIC);
    container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    records = new LinkedBlockingQueue<>();
    container.setupMessageListener((MessageListener<String, String>) records::add);
    container.start();
    ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
  }

  private void initKafkaListener() {
    CONF.set(BOOTSTRAP_SERVERS.key(), embeddedKafkaBroker.getBrokersAsString());
    CONF.set(CLIENT_ID.key(), "apiary-kafka-listener");
    CONF.set(TOPIC_NAME.key(), TOPIC);

    kafkaMetaStoreEventListener = new KafkaMetaStoreEventListener(CONF);
  }
}
