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
package com.expediagroup.dataplatform.dronefly.app.service.listener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingMetastoreListener extends MetaStoreEventListener {
  private static final Logger log = LoggerFactory.getLogger(LoggingMetastoreListener.class);

  public LoggingMetastoreListener(Configuration config) {
    super(config);
  }

  @Override
  public void onAddPartition(AddPartitionEvent addPartitionEvent) throws MetaException {
    log
        .info("Event Type: {}, DB Name: {}, Table Name: {}, Status: {}", EventType.ADD_PARTITION.toString(),
            addPartitionEvent.getTable().getDbName(), addPartitionEvent.getTable().getTableName(),
            addPartitionEvent.getStatus());
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent createDatabaseEvent) throws MetaException {
    log
        .info("Event Type: {}, DB Name: {}, DB Location: {}, Status: {}", EventType.CREATE_DATABASE.toString(),
            createDatabaseEvent.getDatabase().getName(), createDatabaseEvent.getDatabase().getLocationUri(),
            createDatabaseEvent.getStatus());
  }

  @Override
  public void onCreateTable(CreateTableEvent createTableEvent) throws MetaException {
    log
        .info("Event Type: {}, DB Name: {}, Table Name: {}, Status: {}", EventType.CREATE_TABLE.toString(),
            createTableEvent.getTable().getDbName(), createTableEvent.getTable().getTableName(),
            createTableEvent.getStatus());
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dropDatabaseEvent) throws MetaException {
    log
        .info("Event Type: {}, DB Name: {}, DB Location: {}, Status: {}", EventType.DROP_DATABASE.toString(),
            dropDatabaseEvent.getDatabase().getName(), dropDatabaseEvent.getDatabase().getLocationUri(),
            dropDatabaseEvent.getStatus());
  }

  @Override
  public void onDropPartition(DropPartitionEvent dropPartitionEvent) throws MetaException {
    log
        .info("Event Type: {}, DB Name: {}, Table Name: {}, Status: {}", EventType.DROP_PARTITION.toString(),
            dropPartitionEvent.getTable().getDbName(), dropPartitionEvent.getTable().getTableName(),
            dropPartitionEvent.getStatus());
  }

  @Override
  public void onDropTable(DropTableEvent dropTableEvent) throws MetaException {
    log
        .info("Event Type: {}, DB Name: {}, Table Name: {}, Status: {}", EventType.DROP_TABLE.toString(),
            dropTableEvent.getTable().getDbName(), dropTableEvent.getTable().getTableName(),
            dropTableEvent.getStatus());
  }

  @Override
  public void onAlterTable(AlterTableEvent alterTableEvent) throws MetaException {
    log
        .info(
            "Event Type: {}, Old DB Name: {}, Old Table Name: {}, Old Table Location: {}, New DB Name: {}, New Table Name: {}, Old Table Location: {}, Status: {}",
            EventType.ALTER_TABLE.toString(), alterTableEvent.getOldTable().getDbName(),
            alterTableEvent.getOldTable().getTableName(), alterTableEvent.getOldTable().getSd().getLocation(),
            alterTableEvent.getNewTable().getDbName(), alterTableEvent.getNewTable().getTableName(),
            alterTableEvent.getNewTable().getSd().getLocation(), alterTableEvent.getStatus());

  }

  @Override
  public void onAlterPartition(AlterPartitionEvent alterPartitionEvent) throws MetaException {
    log
        .info(
            "Event Type: {}, DB Name: {}, Table Name: {}, Old partition Location: {}, New partition Location: {}, Status: {}",
            EventType.ALTER_PARTITION.toString(), alterPartitionEvent.getOldPartition().getDbName(),
            alterPartitionEvent.getOldPartition().getTableName(),
            alterPartitionEvent.getOldPartition().getSd().getLocation(),
            alterPartitionEvent.getNewPartition().getSd().getLocation(), alterPartitionEvent.getStatus());
  }

  @Override
  public void onCreateFunction(CreateFunctionEvent createFunctionEvent) throws MetaException {
    log
        .info("Event Type: {}, Function Name: {}, Function Class Name: {}, Status: {}",
            EventType.CREATE_FUNCTION.toString(), createFunctionEvent.getFunction().getFunctionName(),
            createFunctionEvent.getFunction().getClassName(), createFunctionEvent.getStatus());
  }

  @Override
  public void onDropFunction(DropFunctionEvent dropFunctionEvent) throws MetaException {
    log
        .info("Event Type: {}, Function Name: {}, Function Class Name: {}, Status: {}",
            EventType.DROP_FUNCTION.toString(), dropFunctionEvent.getFunction().getClassName(),
            dropFunctionEvent.getFunction().getClassName(), dropFunctionEvent.getStatus());
  }

}
