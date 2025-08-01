/**
 * Copyright (C) 2020-2025 Expedia, Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

public class DummyListener extends MetaStoreEventListener {

  public static final List<ListenerEvent> notifyList = new ArrayList<>();
  public static final Counter EVENT_COUNT_METRIC = Counter.builder("EVENT_COUNT_CUSTOM_METRIC")
      .register(Metrics.globalRegistry);

  /**
   * @return The last event received, or null if no event was received.
   */
  public static ListenerEvent getLastEvent() {
    if (notifyList.isEmpty()) {
      return null;
    } else {
      return notifyList.get(notifyList.size() - 1);
    }
  }

  /**
   * @return event at index i, or null if no event was received.
   */
  public static ListenerEvent get(int index) {
    if (notifyList.isEmpty()) {
      return null;
    } else {
      ListenerEvent listenerEvent = notifyList.get(index);
      EVENT_COUNT_METRIC.increment();
      return listenerEvent;
    }
  }

  public static int getNumEvents() {
    return notifyList.size();
  }

  public static void reset() {
    notifyList.clear();
  }

  public DummyListener(Configuration config) {
    super(config);
  }

  @Override
  public void onConfigChange(ConfigChangeEvent configChange) {
    addEvent(configChange);
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    addEvent(partitionEvent);
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent db) throws MetaException {
    addEvent(db);
  }

  @Override
  public void onCreateTable(CreateTableEvent table) throws MetaException {
    addEvent(table);
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent db) throws MetaException {
    addEvent(db);
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    addEvent(partitionEvent);
  }

  @Override
  public void onDropTable(DropTableEvent table) throws MetaException {
    addEvent(table);
  }

  @Override
  public void onAlterTable(AlterTableEvent event) throws MetaException {
    addEvent(event);
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent event) throws MetaException {
    addEvent(event);
  }

  @Override
  public void onLoadPartitionDone(LoadPartitionDoneEvent partitionEvent) throws MetaException {
    addEvent(partitionEvent);
  }

  @Override
  public void onCreateFunction(CreateFunctionEvent fnEvent) throws MetaException {
    addEvent(fnEvent);
  }

  @Override
  public void onDropFunction(DropFunctionEvent fnEvent) throws MetaException {
    addEvent(fnEvent);
  }

  private void addEvent(ListenerEvent event) {
    notifyList.add(event);
  }
}
