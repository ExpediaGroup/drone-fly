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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.apiary.extensions.events.metastore.common.MetaStoreEventsException;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryListenerEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryListenerEventFactory;
import com.expediagroup.dataplatform.dronefly.app.messaging.MessageReaderAdapter;
import com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener;
import com.expediagroup.dataplatform.dronefly.core.exception.DroneFlyException;

@ExtendWith(MockitoExtension.class)
public class DroneFlyNotificationServiceTest {
  private @Mock MessageReaderAdapter reader;
  private @Mock ListenerCatalog listenerCatalog;
  private @Mock HiveEventConverterService converterService;

  private final DummyListener dummyListener = new DummyListener(new HiveConf());
  private final List<MetaStoreEventListener> metastoreListeners = new ArrayList<MetaStoreEventListener>();
  private DroneFlyNotificationService droneFlyNotificationService;
  private CreateTableEvent createTableEvent;
  private ApiaryListenerEvent apiaryCreateTableEvent;

  @BeforeEach
  public void init() throws MetaException, NoSuchObjectException {
    createTableEvent = createTableEvent();
    apiaryCreateTableEvent = createApiaryListenerEvent(createTableEvent);

    when(reader.read()).thenReturn(apiaryCreateTableEvent);

    droneFlyNotificationService = new DroneFlyNotificationService(reader, converterService, listenerCatalog);
  }

  @Test
  public void typical() throws IOException, MetaException, NoSuchObjectException {
    metastoreListeners.add(dummyListener);
    when(converterService.toHiveEvent(apiaryCreateTableEvent)).thenReturn(createTableEvent);
    when(listenerCatalog.getListeners()).thenReturn(metastoreListeners);
    droneFlyNotificationService.notifyListeners();
    verify(reader).read();
    verify(listenerCatalog).getListeners();

    CreateTableEvent actual = (CreateTableEvent) dummyListener.getLastEvent();

    assertEvent(actual);
    destroy();
    verify(reader).close();
  }

  @Test
  public void multipleListenersLoaded() throws IOException, MetaException, NoSuchObjectException {
    when(converterService.toHiveEvent(apiaryCreateTableEvent)).thenReturn(createTableEvent);
    DummyListener listener2 = new DummyListener(new HiveConf());
    metastoreListeners.add(dummyListener);
    metastoreListeners.add(listener2);
    when(listenerCatalog.getListeners()).thenReturn(metastoreListeners);

    droneFlyNotificationService.notifyListeners();
    verify(reader, times(1)).read();
    verify(listenerCatalog, times(1)).getListeners();

    CreateTableEvent actual1 = (CreateTableEvent) dummyListener.getLastEvent();
    CreateTableEvent actual2 = (CreateTableEvent) listener2.getLastEvent();

    assertEvent(actual1);
    assertEvent(actual2);

    destroy();
    verify(reader).close();
  }

  @Test
  public void exceptionThrownWhileDeserializingEvent() throws IOException {
    when(reader.read()).thenThrow(new MetaStoreEventsException("Cannot deserialize hive event"));

    DroneFlyException exception = assertThrows(DroneFlyException.class, () -> {
      droneFlyNotificationService.notifyListeners();
    });

    assertTrue(exception.getMessage().contains("Cannot unmarshal this event. It will be ignored."));

    destroy();
    verify(reader).close();
  }

  @Test
  public void metaExceptionThrownWhileNotifying() throws MetaException, NoSuchObjectException, IOException {
    when(converterService.toHiveEvent(Mockito.any())).thenThrow(new MetaException("MetaException is thrown."));

    DroneFlyException exception = assertThrows(DroneFlyException.class, () -> {
      droneFlyNotificationService.notifyListeners();
    });

    assertTrue(
        exception.getMessage().contains("Hive event was received but Drone Fly failed to notify all the listeners."));

    destroy();
    verify(reader).close();
  }

  @Test
  public void noSuchObjectExceptionThrownWhileNotifying() throws MetaException, NoSuchObjectException, IOException {
    when(converterService.toHiveEvent(Mockito.any()))
        .thenThrow(new NoSuchObjectException("NoSuchObjectException is thrown."));

    DroneFlyException exception = assertThrows(DroneFlyException.class, () -> {
      droneFlyNotificationService.notifyListeners();
    });

    assertTrue(
        exception.getMessage().contains("Hive event was received but Drone Fly failed to notify all the listeners."));

    destroy();
    verify(reader).close();
  }

  @Test
  public void eventNotSupportedByConverter() throws MetaException, NoSuchObjectException, IOException {
    when(converterService.toHiveEvent(any())).thenThrow(new DroneFlyException("Unsupported event type: DROP_INDEX"));

    DroneFlyException exception = assertThrows(DroneFlyException.class, () -> {
      droneFlyNotificationService.notifyListeners();
    });
    assertTrue(exception.getMessage().contains("Unsupported event type: DROP_INDEX"));

    destroy();
    verify(reader).close();
  }

  @Test
  public void noListenersLoaded() throws IOException, MetaException, NoSuchObjectException {
    when(listenerCatalog.getListeners()).thenReturn(new ArrayList<MetaStoreEventListener>());
    when(converterService.toHiveEvent(apiaryCreateTableEvent)).thenReturn(createTableEvent);
    droneFlyNotificationService.notifyListeners();
    verify(reader).read();
    verify(listenerCatalog).getListeners();

    CreateTableEvent actual = (CreateTableEvent) dummyListener.getLastEvent();

    assertThat(actual).isNull();

    destroy();
    verify(reader).close();
  }

  private void assertEvent(CreateTableEvent event) {
    assertThat(event.getTable().getDbName()).isEqualTo("test_db");
    assertThat(event.getTable().getTableName()).isEqualTo("test_table");
    assertThat(event.getTable().getSd().getLocation()).isEqualTo("s3://test_location");
  }

  private CreateTableEvent createTableEvent() throws MetaException {
    CreateTableEvent event = new CreateTableEvent(
        HiveTableTestUtils.createPartitionedTable("test_db", "test_table", "s3://test_location"), true,
        new HMSHandler("test", new HiveConf(), false));
    return event;
  }

  private ApiaryListenerEvent createApiaryListenerEvent(CreateTableEvent event) throws MetaException {
    return new ApiaryListenerEventFactory().create(event);
  }

  private void destroy() throws IOException {
    droneFlyNotificationService.close();
  }
}
