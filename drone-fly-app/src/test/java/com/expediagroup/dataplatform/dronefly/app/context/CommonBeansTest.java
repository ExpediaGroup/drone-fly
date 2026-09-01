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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

public class CommonBeansTest {

  @Test
  public void keyDeserializerDefaultsToLong() {
    assertThat(CommonBeans.keyDeserializer(new Properties())).isEqualTo(LongDeserializer.class.getName());
  }

  @Test
  public void keyDeserializerUsesConfiguredValue() {
    Properties consumerProperties = new Properties();
    consumerProperties
        .setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    assertThat(CommonBeans.keyDeserializer(consumerProperties)).isEqualTo(StringDeserializer.class.getName());
  }
}
