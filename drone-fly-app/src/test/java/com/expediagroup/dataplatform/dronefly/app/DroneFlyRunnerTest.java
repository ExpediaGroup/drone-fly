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
package com.expediagroup.dataplatform.dronefly.app;

import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.awaitility.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.ApplicationArguments;

import com.expediagroup.dataplatform.dronefly.app.service.DroneFlyNotificationService;

@ExtendWith(MockitoExtension.class)
public class DroneFlyRunnerTest {

  @Mock
  private ApplicationArguments args;

  private @Mock DroneFlyNotificationService droneFlyNotificationService;

  private DroneFlyRunner runner;
  private final ExecutorService executor = Executors.newFixedThreadPool(1);

  @BeforeEach
  public void init() {
    runner = new DroneFlyRunner(droneFlyNotificationService);
  }

  @Test
  public void typical() throws IOException, InterruptedException {
    runRunner();
    await()
        .atMost(Duration.FIVE_SECONDS)
        .untilAsserted(() -> {
              verify(droneFlyNotificationService, atLeast(1)).notifyListeners();
            }
        );
    destroy();
    verify(droneFlyNotificationService).close();
  }

  @Test
  public void typicalRunWithException() throws Exception {
    doNothing().doThrow(new RuntimeException()).doNothing().when(droneFlyNotificationService).notifyListeners();
    runRunner();
    await()
        .atMost(Duration.FIVE_SECONDS)
        .untilAsserted(() -> {
              verify(droneFlyNotificationService, atLeast(3)).notifyListeners();
            }
        );
    destroy();
    verify(droneFlyNotificationService).close();
  }

  private void runRunner() {
    executor.execute(() -> {
      try {
        runner.run(args);
      } catch (Exception e) {
        fail("Exception thrown on run");
      }
    });
  }

  private void destroy() throws InterruptedException {
    runner.destroy();
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }
}
