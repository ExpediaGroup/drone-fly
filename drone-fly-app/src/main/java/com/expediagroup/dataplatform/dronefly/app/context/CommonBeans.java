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
package com.expediagroup.dataplatform.dronefly.app.context;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessageReader;
import com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessageReader.KafkaMessageReaderBuilder;
import com.expediagroup.dataplatform.dronefly.app.messaging.MessageReaderAdapter;
import com.expediagroup.dataplatform.dronefly.app.service.ListenerCatalog;
import com.expediagroup.dataplatform.dronefly.app.service.factory.ListenerCatalogFactory;

@Configuration
public class CommonBeans {

  private static final Logger log = LoggerFactory.getLogger(CommonBeans.class);
  public static final String CONSUMER_PROPERTIES_PREFIX = "apiary.messaging.consumer";

  @Value("${instance.name:drone-fly}")
  private String instanceName;

  @Value("${apiary.bootstrap.servers}")
  private String bootstrapServers;

  @Value("${apiary.kafka.topic.name}")
  private String topicName;

  @Value("${apiary.listener.list:}")
  private String confListenerList;

  @Bean
  public HiveConf hiveConf() {
    return new HiveConf();
  }

  @Bean
  @Primary
  @ConfigurationProperties(CONSUMER_PROPERTIES_PREFIX)
  public Properties getEnvProperties() {
    return new Properties();
  }

  @Bean
  public ListenerCatalog listenerCatalog(HiveConf conf) throws MetaException {
    ListenerCatalog listenerCatalog = new ListenerCatalogFactory(conf).newInstance(confListenerList);
    List<MetaStoreEventListener> listenerList = listenerCatalog.getListeners();
    String listeners = listenerList.stream().map(x -> x.getClass().getName()).collect(Collectors.joining(", "));
    log.info("DroneFly is starting with {} listeners: {}", listenerList.size(), listeners);
    return listenerCatalog;
  }

  @Bean
  public MessageReaderAdapter messageReaderAdapter() {
    Properties consumerProperties = getConsumerProperties();
    KafkaMessageReader delegate = KafkaMessageReaderBuilder.
        builder(bootstrapServers, topicName, instanceName).
        withKeyDeserializer(keyDeserializer(consumerProperties)).
        withConsumerProperties(consumerProperties).
        build();
    return new MessageReaderAdapter(delegate);
  }

  /**
   * Resolves the deserializer for the Kafka record key.
   * <p>
   * The key type is decided by whichever producer writes the topic. The Apiary Hive Metastore
   * listener writes a {@code Long}, which is the default here, but a topic populated by another
   * producer may use a different type. Reading a topic with the wrong deserializer fails on every
   * record, and because the consumer offset does not advance past a record it cannot deserialize,
   * the service makes no progress at all.
   * <p>
   * The value has to be read out of the consumer properties and passed to the builder explicitly:
   * properties given to {@code withConsumerProperties} do not override the builder's own defaults.
   *
   * @param consumerProperties consumer properties bound from {@value #CONSUMER_PROPERTIES_PREFIX}
   * @return the configured key deserializer, or {@link LongDeserializer} when unset
   */
  static String keyDeserializer(Properties consumerProperties) {
    return consumerProperties
        .getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
  }

  private Properties getConsumerProperties() {
    Properties consumerProperties = new Properties();
    getEnvProperties().forEach((key, value) -> {
      consumerProperties.put(key.toString(), value.toString());
      log.info("Consumer property {} set with value: {}", key, value);
    });
    return consumerProperties;
  }
}