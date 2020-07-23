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
package com.expediagroup.dataplatform.dronefly.app.service.factory;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.expediagroup.dataplatform.dronefly.app.service.ListenerCatalog;
import com.expediagroup.dataplatform.dronefly.app.service.listener.AnotherDummyListener;

public class ListenerCatalogFactoryTest {

  private ListenerCatalogFactory listenerCatalogFactory;

  @BeforeEach
  public void init() {
    HiveConf hiveConf = new HiveConf();
    listenerCatalogFactory = new ListenerCatalogFactory(hiveConf);
  }

  @Test
  public void listenerImplListProvided() {
    String confProvidedList = "com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener";
    ListenerCatalog listenerCatalog = listenerCatalogFactory.newInstance(confProvidedList);

    assertThat(listenerCatalog.getListeners().size()).isEqualTo(1);
  }

  @Test
  public void listenerImplListFromHiveConf() {
    HiveConf hiveConf = new HiveConf();
    String listenerImplList = "com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener";
    hiveConf.set(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.toString(), listenerImplList);
    ListenerCatalogFactory listenerCatalogFactory = new ListenerCatalogFactory(hiveConf);

    ListenerCatalog listenerCatalog = listenerCatalogFactory.newInstance("");

    assertThat(listenerCatalog.getListeners().size()).isEqualTo(1);
  }

  @Test
  public void configGivenPriorityOverHiveConf() {
    HiveConf hiveConf = new HiveConf();
    String listenerImplList = "com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener";
    hiveConf.set(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.toString(), listenerImplList);
    ListenerCatalogFactory listenerCatalogFactory = new ListenerCatalogFactory(hiveConf);

    ListenerCatalog listenerCatalog = listenerCatalogFactory
        .newInstance("com.expediagroup.dataplatform.dronefly.app.service.listener.AnotherDummyListener");

    assertThat(listenerCatalog.getListeners().size()).isEqualTo(1);
    assertThat(listenerCatalog.getListeners().get(0)).isInstanceOf(AnotherDummyListener.class);
  }

  @Test
  public void listenerImplListNotProvidedInConfOrHiveSite() {
    ListenerCatalog listenerCatalog = listenerCatalogFactory.newInstance("");
    assertThat(listenerCatalog.getListeners().size()).isEqualTo(1);
  }

  @Test
  public void listenerImplListProvidedWithJustWhitespaces() {
    HiveConf hiveConf = new HiveConf();
    String listenerImplList = "com.expediagroup.dataplatform.dronefly.app.service.listener.DummyListener";
    hiveConf.set(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.toString(), listenerImplList);
    ListenerCatalogFactory listenerCatalogFactory = new ListenerCatalogFactory(hiveConf);

    ListenerCatalog listenerCatalog = listenerCatalogFactory.newInstance("");

    assertThat(listenerCatalog.getListeners().size()).isEqualTo(1);
  }

  @Test
  public void nullListenerImplListProvided() {
    ListenerCatalog listenerCatalog = listenerCatalogFactory.newInstance(null);

    assertThat(listenerCatalog.getListeners().size()).isEqualTo(1);
  }

}
