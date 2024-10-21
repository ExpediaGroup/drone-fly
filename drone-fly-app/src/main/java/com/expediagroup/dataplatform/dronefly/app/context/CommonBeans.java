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
package com.expediagroup.dataplatform.dronefly.app.context;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessageReader;
import com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessageReader.KafkaMessageReaderBuilder;
import com.expediagroup.dataplatform.dronefly.app.messaging.MessageReaderAdapter;
import com.expediagroup.dataplatform.dronefly.app.service.ListenerCatalog;
import com.expediagroup.dataplatform.dronefly.app.service.factory.ListenerCatalogFactory;

@Configuration
public class CommonBeans {
  private static final Logger log = LoggerFactory.getLogger(CommonBeans.class);

  @Value("${instance.name:drone-fly}")
  private String instanceName;

  @Value("${apiary.bootstrap.servers}")
  private String bootstrapServers;

  @Value("${apiary.kafka.topic.name}")
  private String topicName;

  @Value("${apiary.listener.list:}")
  private String confListenerList;

  @Value("${apiary.security.protocol:#{null}}")
  private String securityProtol;

  @Value("${apiary.sasl.mechanism:#{null}}")
  private String saslMechanism;

  @Value("${apiary.sasl.jaas.config:#{null}}")
  private String saslJaasConfig;

  @Value("${apiary.sasl.client.callback.handler.class:#{null}}")
  private String saslHandlerClass;

  @Bean
  public HiveConf hiveConf() {
    return new HiveConf();
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
    Properties clientProperties = getClientProperties();

    KafkaMessageReader delegate = KafkaMessageReaderBuilder.
            builder(bootstrapServers, topicName, instanceName).
            withConsumerProperties(clientProperties).
            build();
    return new MessageReaderAdapter(delegate);
  }

  private Properties getClientProperties() {
    Properties clientProperties = new Properties();
    if (StringUtils.isNotBlank(securityProtol)) { clientProperties.put("security.protocol", securityProtol); }
    if (StringUtils.isNotBlank(saslMechanism)) { clientProperties.put("sasl.mechanism", saslMechanism); }
    if (StringUtils.isNotBlank(saslJaasConfig)) { clientProperties.put("sasl.jaas.config", saslJaasConfig); }
    if (StringUtils.isNotBlank(saslHandlerClass)) { clientProperties.put("sasl.client.callback.handler.class",
            saslHandlerClass); }
    return clientProperties;
  }

}