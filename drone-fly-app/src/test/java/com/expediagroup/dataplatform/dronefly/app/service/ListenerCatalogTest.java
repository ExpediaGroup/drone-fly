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

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.jupiter.api.Test;

import com.expediagroup.dataplatform.dronefly.app.service.listener.AnotherDummyListener;
import com.expediagroup.dataplatform.dronefly.app.service.listener.DefaultDroneFlyListener;
import com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener;
import com.expediagroup.dataplatform.dronefly.core.exception.DroneFlyException;

public class ListenerCatalogTest {
  private ListenerCatalog listenerCatalog;

  @Test
  public void typical() throws MetaException {
    String listenerImplList = "com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener,"
        + "com.expediagroup.dataplatform.dronefly.app.service.listener.AnotherDummyListener";

    listenerCatalog = new ListenerCatalog(new HiveConf(), listenerImplList);
    List<MetaStoreEventListener> result = listenerCatalog.getListeners();

    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(0)).isInstanceOf(DummyListener.class);
    assertThat(result.get(1)).isInstanceOf(AnotherDummyListener.class);
  }

  @Test
  public void oneListenerProvidedAndNotFound() throws MetaException {
    String listenerImplList = "com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener1";
    DroneFlyException exception = assertThrows(DroneFlyException.class, () -> {
      listenerCatalog = new ListenerCatalog(new HiveConf(), listenerImplList);
    });

    assertTrue(exception
        .getMessage()
        .contains(
            "Failed to instantiate listener named: com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener1"));
  }

  @Test
  public void oneOutOfTwoListenersNotFound() throws MetaException {
    String listenerImplList = "com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener,"
        + "com.expediagroup.dataplatform.dronefly.app.service.listener.AnotherDummyListener1";
    DroneFlyException exception = assertThrows(DroneFlyException.class, () -> {
      listenerCatalog = new ListenerCatalog(new HiveConf(), listenerImplList);
    });

    assertTrue(exception
        .getMessage()
        .contains(
            "Failed to instantiate listener named: com.expediagroup.dataplatform.dronefly.app.service.listener.AnotherDummyListener1"));
  }

  @Test
  public void whiteSpacesInTheMiddleOfListenerImplList() throws MetaException {
    String listenerImplList = "com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener         ,"
        + "       com.expediagroup.dataplatform.dronefly.app.service.listener.AnotherDummyListener";
    listenerCatalog = new ListenerCatalog(new HiveConf(), listenerImplList);
    List<MetaStoreEventListener> result = listenerCatalog.getListeners();

    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(0)).isInstanceOf(DummyListener.class);
    assertThat(result.get(1)).isInstanceOf(AnotherDummyListener.class);
  }

  @Test
  public void extraCommaAtTheEndOfListenerImplList() throws MetaException {
    String listenerImplList = "com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener,"
        + "com.expediagroup.dataplatform.dronefly.app.service.listener.AnotherDummyListener,";
    listenerCatalog = new ListenerCatalog(new HiveConf(), listenerImplList);
    List<MetaStoreEventListener> result = listenerCatalog.getListeners();

    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(0)).isInstanceOf(DummyListener.class);
    assertThat(result.get(1)).isInstanceOf(AnotherDummyListener.class);
  }

  @Test
  public void emptyListenerImplList() throws MetaException {
    String listenerImplList = "   ";
    listenerCatalog = new ListenerCatalog(new HiveConf(), listenerImplList);
    List<MetaStoreEventListener> result = listenerCatalog.getListeners();
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0)).isInstanceOf(DefaultDroneFlyListener.class);
  }

  @Test
  public void nullListenerImplList() throws MetaException {
    String listenerImplList = null;
    listenerCatalog = new ListenerCatalog(new HiveConf(), listenerImplList);
    List<MetaStoreEventListener> result = listenerCatalog.getListeners();
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0)).isInstanceOf(DefaultDroneFlyListener.class);
  }

}
