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
package com.expediagroup.dataplatform.dronefly.app.service;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.expediagroup.apiary.extensions.events.metastore.common.MetaStoreEventsException;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryListenerEvent;
import com.expediagroup.dataplatform.dronefly.app.messaging.MessageReaderAdapter;
import com.expediagroup.dataplatform.dronefly.core.exception.DroneFlyException;

@Component
public class DroneFlyNotificationService {
  private static final Logger log = LoggerFactory.getLogger(DroneFlyNotificationService.class);
  private final MessageReaderAdapter reader;
  private final HiveEventConverterService converterService;
  private final ListenerCatalog listenerCatalog;

  @Autowired
  public DroneFlyNotificationService(
      MessageReaderAdapter reader,
      HiveEventConverterService converterService,
      ListenerCatalog listenerCatalog) {
    this.reader = reader;
    this.converterService = converterService;
    this.listenerCatalog = listenerCatalog;
  }

  public void notifyListeners() throws DroneFlyException {
    try {
      ApiaryListenerEvent event = reader.read();
      ListenerEvent hiveEvent = converterService.toHiveEvent(event);
      List<MetaStoreEventListener> listeners = listenerCatalog.getListeners();
      log.info("Read event: %s", event.getEventType().toString());
      log.info("Listeners being notified: %s", listeners.size());
      // The following class notifies all the listeners loaded in a loop. It will stop notifying if one of the loaded
      // listeners throws an Exception. This is expected behaviour. If Drone Fly is deployed in Kubernetes containers
      // with only one listener loaded per instance, it won't be an issue.
      MetaStoreListenerNotifier.notifyEvent(listeners, getHiveEventType(event), hiveEvent);
    } catch (MetaStoreEventsException e) {
      throw new DroneFlyException("Cannot unmarshal this event. It will be ignored.", e);
    } catch (MetaException | NoSuchObjectException e) {
      throw new DroneFlyException("Hive event was received but Drone Fly failed to notify all the listeners.", e);
    }
  }

  private EventType getHiveEventType(ApiaryListenerEvent event) {
    return EventType.valueOf(event.getEventType().name().substring(3));
  }

  public void close() throws IOException {
    reader.close();
  }

}
