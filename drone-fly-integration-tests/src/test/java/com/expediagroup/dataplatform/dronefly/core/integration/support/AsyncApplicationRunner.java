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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

/**
 * Wraps a blocking {@link ApplicationRunner} in a daemon thread so that Spring Boot startup
 * completes without mocking. Intended for use in {@code @TestConfiguration} beans only.
 *
 * <p>{@code DroneFlyRunner.run()} blocks indefinitely (it is the Kafka polling loop). Spring Boot
 * calls {@code ApplicationRunner} beans synchronously during startup, so {@code @SpringBootTest}
 * would hang. Registering this wrapper via {@code @TestConfiguration} with
 * {@code spring.main.allow-bean-definition-overriding=true} replaces the component-scanned bean
 * so Spring Boot startup completes normally.
 */
class AsyncApplicationRunner implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(AsyncApplicationRunner.class);

  private final ApplicationRunner delegate;
  private final String threadName;

  AsyncApplicationRunner(ApplicationRunner delegate, String threadName) {
    this.delegate = delegate;
    this.threadName = threadName;
  }

  @Override
  public void run(ApplicationArguments args) {
    Thread thread = new Thread(() -> {
      try {
        delegate.run(args);
      } catch (Exception e) {
        log.warn("{} delegate threw unexpectedly", threadName, e);
      }
    }, threadName);
    thread.setDaemon(true);
    thread.start();
  }
}
