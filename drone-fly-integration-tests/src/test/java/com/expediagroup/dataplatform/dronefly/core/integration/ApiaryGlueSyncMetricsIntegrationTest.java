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
import static org.assertj.core.api.Assertions.assertThat;

import static com.expediagroup.dataplatform.dronefly.core.integration.support.DroneFlyIntegrationTestUtils.buildPartition;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.DroneFlyIntegrationTestUtils.buildTable;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.SpringMetricsUtils.metric;
import static com.expediagroup.dataplatform.dronefly.core.integration.support.SpringMetricsUtils.springMetricsIncrease;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
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
import org.springframework.kafka.test.context.EmbeddedKafka;

import io.micrometer.core.instrument.Metrics;

import com.expediagroup.apiary.extensions.gluesync.listener.ApiaryGlueSync;
import com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricConstants;
import com.expediagroup.dataplatform.dronefly.app.DroneFly;
import com.expediagroup.dataplatform.dronefly.app.service.ListenerCatalog;
import com.expediagroup.dataplatform.dronefly.core.integration.support.AsyncRunnerConfig;
import com.expediagroup.dataplatform.dronefly.core.integration.support.SpringMetricsUtils;

/**
 * Deployment smoke test for apiary-gluesync-listener inside a Dronefly Spring Boot context.
 * Validates two things that cannot be covered in apiary-gluesync-listener without a circular
 * dependency: (1) the fat jar loads without classpath conflicts, and (2) Prometheus is registered
 * before ApiaryGlueSync is constructed so MetricService does not fall back to JMX.
 */
@SpringBootTest(
    classes = DroneFly.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.main.allow-bean-definition-overriding=true",
        "apiary.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "apiary.kafka.topic.name=test-topic",
        "instance.name=test",
        "apiary.listener.list=com.expediagroup.apiary.extensions.gluesync.listener.ApiaryGlueSync",
        // Explicitly enable Prometheus so PrometheusMeterRegistry is added to
        // Metrics.globalRegistry before ApiaryGlueSync is constructed.
        "management.metrics.export.prometheus.enabled=true",
        "management.endpoints.web.exposure.include=metrics,prometheus"
    }
)
@EmbeddedKafka(controlledShutdown = true, topics = {"test-topic"}, partitions = 1)
@Import(AsyncRunnerConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApiaryGlueSyncMetricsIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(DroneFlyIntegrationTest.class);

  private static final SpringMetricsUtils.Metric CREATE_TABLE_IGNORED = metric("glue_listener_event", "COUNT",
      "operation", MetricConstants.CREATE_TABLE,
      "result", MetricConstants.RESULT_IGNORED,
      "outcome", "ignored");
  private static final SpringMetricsUtils.Metric ADD_PARTITION_IGNORED = metric("glue_listener_event", "COUNT",
      "operation", MetricConstants.ADD_PARTITION,
      "result", MetricConstants.RESULT_IGNORED,
      "outcome", "ignored");
  private static final SpringMetricsUtils.Metric TABLE_FAILURE = metric("glue_listener_table_failure", "COUNT");
  private static final SpringMetricsUtils.Metric CREATE_TABLE_FAILURE = metric("glue_listener_event", "COUNT",
      "operation", MetricConstants.CREATE_TABLE,
      "result", MetricConstants.RESULT_FAILURE);

  @LocalManagementPort
  private int managementPort;

  @Autowired
  ListenerCatalog listenerCatalog;

  @Autowired
  TestRestTemplate restTemplate;

  @BeforeAll
  void setUp() {
    log.info("Management URI: http://localhost:{}/actuator", managementPort);
  }

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

  /**
   * Verifies all GlueSync counters are exported via the actuator metrics endpoint,
   * confirming they landed in the Prometheus registry rather than a hidden JMX one.
   * {@code glue_listener_event} is on-demand, so a status=false event is fired first
   * to ensure it is registered before the name list is checked.
   */
  @Test
  void gluesyncMetricsExported() {
    ApiaryGlueSync apiaryGlueSync = listenerCatalog.getListeners().stream()
        .filter(ApiaryGlueSync.class::isInstance)
        .map(ApiaryGlueSync.class::cast)
        .findFirst()
        .orElseThrow();
    try {
      apiaryGlueSync.onCreateTable(new CreateTableEvent(buildTable(), false, Mockito.mock(HMSHandler.class)));
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }

    assertThat(SpringMetricsUtils.metricNames(restTemplate))
        .containsAll(MetricConstants.LISTENER_METRICS)
        .contains(MetricConstants.LISTENER_EVENT);
  }

  /**
   * Verifies GlueSync counters appear in the Prometheus scrape output.
   * Micrometer appends {@code _total} to counter names in Prometheus format.
   * {@code glue_listener_event} is registered on-demand, so a status=false event
   * is fired first to ensure it is present in the registry before scraping.
   */
  @Test
  void gluesyncMetricsInPrometheusFormat() {
    ApiaryGlueSync apiaryGlueSync = listenerCatalog.getListeners().stream()
        .filter(ApiaryGlueSync.class::isInstance)
        .map(ApiaryGlueSync.class::cast)
        .findFirst()
        .orElseThrow();
    try {
      apiaryGlueSync.onCreateTable(new CreateTableEvent(buildTable(), false, Mockito.mock(HMSHandler.class)));
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }

    String body = SpringMetricsUtils.prometheusBody(restTemplate);
    MetricConstants.LISTENER_METRICS.forEach(name -> assertThat(body).contains(name + "_total"));
    assertThat(body).contains(MetricConstants.LISTENER_EVENT + "_total");
  }

  /**
   * Verifies that the new tagged {@code glue_listener_event} counter is recorded and exported when
   * events are processed. Uses {@code status=false} events so the HMS-failure path is taken
   * inside ApiaryGlueSync — which records the "ignored" metric without making any Glue API calls.
   */
  @Test
  void gluesyncEventMetricRecorded() {
    ApiaryGlueSync apiaryGlueSync = listenerCatalog.getListeners().stream()
        .filter(ApiaryGlueSync.class::isInstance)
        .map(ApiaryGlueSync.class::cast)
        .findFirst()
        .orElseThrow();

    HMSHandler hmsHandler = Mockito.mock(HMSHandler.class);

    springMetricsIncrease(
        restTemplate,
        () -> {
          try {
            apiaryGlueSync.onCreateTable(new CreateTableEvent(buildTable(), false, hmsHandler));
            apiaryGlueSync.onAddPartition(new AddPartitionEvent(buildTable(), buildPartition(), false, hmsHandler));
          } catch (MetaException e) {
            throw new RuntimeException(e);
          }
        },
        entry(CREATE_TABLE_IGNORED, 1.0),
        entry(ADD_PARTITION_IGNORED, 1.0)
    );
  }

  /**
   * Verifies {@code glue_listener_table_failure} and the tagged {@code glue_listener_event}
   * failure counter are recorded when the Glue API is unreachable. The surefire configuration
   * sets {@code AWS_REGION=us-fake-1} with fake credentials so every real Glue call fails fast
   * with an {@code UnknownHostException} — no mocking needed.
   */
  @Test
  void gluesyncFailureMetricRecordedOnGlueError() {
    ApiaryGlueSync apiaryGlueSync = listenerCatalog.getListeners().stream()
        .filter(ApiaryGlueSync.class::isInstance)
        .map(ApiaryGlueSync.class::cast)
        .findFirst()
        .orElseThrow();

    HMSHandler hmsHandler = Mockito.mock(HMSHandler.class);

    springMetricsIncrease(
        restTemplate,
        () -> {
          try {
            apiaryGlueSync.onCreateTable(new CreateTableEvent(buildTable(), true, hmsHandler));
          } catch (MetaException e) {
            throw new RuntimeException(e);
          }
        },
        entry(TABLE_FAILURE, 1.0),
        entry(CREATE_TABLE_FAILURE, 1.0));
  }
}
