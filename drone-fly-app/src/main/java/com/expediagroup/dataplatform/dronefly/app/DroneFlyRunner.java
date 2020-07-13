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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.expediagroup.dataplatform.dronefly.app.service.DroneFlyNotificationService;
import com.expediagroup.dataplatform.dronefly.core.exception.DroneFlyException;

@Component
public class DroneFlyRunner implements ApplicationRunner {
  private static final Logger log = LoggerFactory.getLogger(DroneFlyRunner.class);
  private final DroneFlyNotificationService droneFlyNotificationService;
  private final AtomicBoolean running = new AtomicBoolean(false);

  @Autowired
  public DroneFlyRunner(DroneFlyNotificationService droneFlyNotificationService) {
    this.droneFlyNotificationService = droneFlyNotificationService;
  }

  @Override
  public void run(ApplicationArguments args) {
    running.set(true);
    while (running.get()) {
      try {
        droneFlyNotificationService.notifyListeners();
      } catch (Exception e) {
        log.error("Problem processing this event.", e);
      }
    }
    log.info("Drone Fly has stopped");
  }

  @PreDestroy
  public void destroy() {
    log.info("Shutting down Drone Fly...");
    running.set(false);
    try {
      droneFlyNotificationService.close();
    } catch (IOException e) {
      throw new DroneFlyException("Problem closing notification service.", e);
    }

    log.info("Drone Fly shutdown complete.");
  }

}
