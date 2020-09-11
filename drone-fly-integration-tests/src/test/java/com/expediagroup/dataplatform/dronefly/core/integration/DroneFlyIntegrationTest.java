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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.CLIENT_ID;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.TOPIC_NAME;
import static com.expediagroup.dataplatform.dronefly.core.integration.DroneFlyIntegrationTestUtils.buildTable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.awaitility.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.expediagroup.apiary.extensions.events.metastore.kafka.listener.KafkaMetaStoreEventListener;
import com.expediagroup.dataplatform.dronefly.app.DroneFly;

@EmbeddedKafka(controlledShutdown = true, topics = { "domain-events" }, partitions = 1)
@ExtendWith(SpringExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DroneFlyIntegrationTest {
  private static final String TOPIC = "domain-events";

  private @Mock HMSHandler hmsHandler;

  protected final ExecutorService executorService = Executors.newFixedThreadPool(1);
  private static Configuration CONF = new Configuration();

  private KafkaMetaStoreEventListener kafkaMetaStoreEventListener;

  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  //
  // BlockingQueue<ConsumerRecord<String, String>> records;
  //
  // KafkaMessageListenerContainer<String, String> container;

  @BeforeAll
  void setUp() {
    // // Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("consumer", "false",
    // // embeddedKafkaBroker));
    // // DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs,
    // // new StringDeserializer(), new StringDeserializer());
    // ContainerProperties containerProperties = new ContainerProperties(TOPIC);
    // container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    // // records = new LinkedBlockingQueue<>();
    // // container.setupMessageListener((MessageListener<String, String>) records::add);
    // container.start();
    // ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());

    System.setProperty("instance.name", "abhi-2");
    System.setProperty("apiary.bootstrap.servers", embeddedKafkaBroker.getBrokersAsString());
    System.setProperty("apiary.kafka.topic.name", TOPIC);
    System.setProperty("apiary.listener.list", "com.expediagroup.dataplatform.dronefly.core.integration.DummyListener");
    initKafkaListener();
  }

  private void initKafkaListener() {
    CONF.set(BOOTSTRAP_SERVERS.key(), embeddedKafkaBroker.getBrokersAsString());
    CONF.set(CLIENT_ID.key(), "apiary-kafka-listner");
    CONF.set(TOPIC_NAME.key(), TOPIC);

    kafkaMetaStoreEventListener = new KafkaMetaStoreEventListener(CONF);

  }

  @BeforeEach
  public void setup() {
    executorService.execute(() -> DroneFly.main(new String[] {}));
    await().atMost(Duration.TEN_MINUTES).until(DroneFly::isRunning);
  }

  @AfterEach
  public void stop() throws InterruptedException {
    DroneFly.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @AfterAll
  void tearDown() {
    // container.stop();
  }

  @Test
  public void kafkaSetup_withTopic_ensureSendMessageIsReceived() throws Exception {

    // await().atMost(Duration.TEN_MINUTES).until(() -> embeddedKafkaBroker.getPartitionsPerTopic() == 1);

    Thread.sleep(1000);

    DropTableEvent dropTableEvent = new DropTableEvent(buildTable(), true, false, hmsHandler);
    kafkaMetaStoreEventListener.onDropTable(dropTableEvent);

    CreateTableEvent createTableEvent = new CreateTableEvent(buildTable(), true, hmsHandler);
    kafkaMetaStoreEventListener.onCreateTable(createTableEvent);

    await().atMost(5, TimeUnit.SECONDS).until(() -> DummyListener.getNumEvents() > 1);

    ListenerEvent event = DummyListener.getLastEvent();

    assertThat(DummyListener.getNumEvents()).isEqualTo(2);
    assertThat(event.getStatus()).isTrue();

  }

}
