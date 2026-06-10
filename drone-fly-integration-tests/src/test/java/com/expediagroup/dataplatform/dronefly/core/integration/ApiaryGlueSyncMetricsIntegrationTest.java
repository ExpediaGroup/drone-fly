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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import io.micrometer.core.instrument.Metrics;

import com.expediagroup.dataplatform.dronefly.app.DroneFly;
import com.expediagroup.dataplatform.dronefly.app.DroneFlyRunner;
import com.expediagroup.dataplatform.dronefly.app.service.ListenerCatalog;

/**
 * Deployment smoke test for apiary-gluesync-listener inside a Dronefly Spring Boot context.
 * Validates two things that cannot be covered in apiary-gluesync-listener without a circular
 * dependency: (1) the fat jar loads without classpath conflicts, and (2) Prometheus is registered
 * before ApiaryGlueSync is constructed so MetricService does not fall back to JMX.
 */
@SpringBootTest(
    classes = DroneFly.class,
    // RANDOM_PORT (not NONE) mirrors production: Dronefly always runs a web server, and the
    // web server's dependency chain causes PrometheusMeterRegistry to initialise before
    // ListenerCatalog — preventing the JmxMeterRegistry fallback in MetricService.
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "apiary.bootstrap.servers=localhost:9999",
        "apiary.kafka.topic.name=test-topic",
        "instance.name=test",
        "apiary.listener.list=com.expediagroup.apiary.extensions.gluesync.listener.ApiaryGlueSync",
        // Spring Boot test defaults disable metric export; re-enable so PrometheusMeterRegistry
        // is added to Metrics.globalRegistry before ApiaryGlueSync is constructed.
        "management.defaults.metrics.export.enabled=true",
        "management.prometheus.metrics.export.enabled=true"
    }
)
class ApiaryGlueSyncMetricsIntegrationTest {

  @MockitoBean
  DroneFlyRunner droneFlyRunner;

  @Autowired
  ListenerCatalog listenerCatalog;

  /** Verifies the fat jar loaded cleanly and that Prometheus was registered before ApiaryGlueSync
   *  was constructed — wrong bean ordering would silently add a JmxMeterRegistry instead. */
  @Test
  void listenerLoadedWithCorrectMetricRegistry() {
    assertThat(listenerCatalog.getListeners())
        .extracting(l -> l.getClass().getSimpleName())
        .contains("ApiaryGlueSync");

    assertThat(Metrics.globalRegistry.getRegistries())
        .noneMatch(r -> r.getClass().getName().contains("JmxMeterRegistry"));
  }
}
