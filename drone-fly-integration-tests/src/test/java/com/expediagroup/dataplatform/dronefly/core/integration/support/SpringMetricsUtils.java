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

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.api.SoftAssertions;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import static org.awaitility.Awaitility.await;

import com.jayway.jsonpath.JsonPath;

/**
 * Test utilities for asserting that Spring Boot actuator metrics increment as expected when
 * a function is invoked. Use {@link #springMetricsIncrease} to snapshot metric values before
 * calling a function, then poll until each metric has risen by the expected delta.
 */
public class SpringMetricsUtils {

  public record Metric(String name, String statisticName, List<String> tags) {
    public String[] tagsArray() {
      return tags.toArray(new String[0]);
    }
  }

  public static Metric metric(String name, String statisticName, String... tags) {
    return new Metric(name, statisticName, List.of(tags));
  }

  /**
   * Returns the value for the given statistic of a named actuator metric.
   * Returns 0.0 if the metric or statistic is not found.
   */
  public static double metricValue(TestRestTemplate restTemplate, String name, String statistic, String... tags) {
    try {
      UriComponentsBuilder builder = UriComponentsBuilder.fromPath("/actuator/metrics/{name}");
      for (int i = 0; i + 1 < tags.length; i += 2) {
        builder.queryParam("tag", tags[i] + ":" + tags[i + 1]);
      }
      URI uri = builder.buildAndExpand(name).toUri();
      String body = restTemplate.getForObject(uri, String.class);
      List<Double> values = JsonPath.read(body, "$.measurements[?(@.statistic=='" + statistic + "')].value");
      return values.isEmpty() ? 0.0 : values.get(0);
    } catch (Exception e) {
      return 0.0;
    }
  }

  /** Returns all metric names exposed by the actuator. */
  public static List<String> metricNames(TestRestTemplate restTemplate) {
    String body = restTemplate.getForObject("/actuator/metrics", String.class);
    return JsonPath.read(body, "$.names");
  }

  /** Returns the raw Prometheus scrape text from {@code /actuator/prometheus}. */
  public static String prometheusBody(TestRestTemplate restTemplate) {
    return restTemplate.getForObject("/actuator/prometheus", String.class);
  }

  /**
   * Executes {@code fn} then verifies each metric increased by the expected amount.
   * Polls via Awaitility to handle async propagation.
   */
  @SafeVarargs
  public static void springMetricsIncrease(
      TestRestTemplate restTemplate,
      Runnable fn,
      Map.Entry<Metric, Double>... metricIncreases) {
    Map<Metric, Double> increaseMap = Arrays.stream(metricIncreases)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<Metric, Double> before = increaseMap.keySet().stream()
        .collect(Collectors.toMap(
            m -> m,
            m -> metricValue(restTemplate, m.name(), m.statisticName(), m.tagsArray())));

    fn.run();

    await().atMost(1, TimeUnit.MINUTES).untilAsserted(() ->
        SoftAssertions.assertSoftly(softly ->
            before.forEach((metric, originalValue) ->
                softly.assertThat(metricValue(restTemplate, metric.name(), metric.statisticName(), metric.tagsArray()))
                    .as("Expecting metric %s to have increased by %s. Original value was %s",
                        metric.name(), increaseMap.get(metric), originalValue)
                    .isEqualTo(originalValue + increaseMap.get(metric)))));
  }
}
