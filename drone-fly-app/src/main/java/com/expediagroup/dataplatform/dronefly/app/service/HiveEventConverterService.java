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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryAddPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryAlterPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryAlterTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryCreateTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryDropPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryDropTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryInsertEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryListenerEvent;
import com.expediagroup.dataplatform.dronefly.app.service.factory.HMSHandlerFactory;
import com.expediagroup.dataplatform.dronefly.core.exception.DroneFlyException;

@Component
public class HiveEventConverterService {
  private static final Logger log = LoggerFactory.getLogger(HiveEventConverterService.class);

  private final HMSHandlerFactory hmsHandlerFactory;

  @Autowired
  public HiveEventConverterService(HMSHandlerFactory hmsHandlerFactory) {
    this.hmsHandlerFactory = hmsHandlerFactory;
  }

  public ListenerEvent toHiveEvent(ApiaryListenerEvent serializableHiveEvent)
    throws MetaException, NoSuchObjectException {
    ListenerEvent hiveEvent = null;

    if (serializableHiveEvent == null) {
      return hiveEvent;
    }

    switch (serializableHiveEvent.getEventType()) {
    case ON_ADD_PARTITION: {
      ApiaryAddPartitionEvent addPartition = (ApiaryAddPartitionEvent) serializableHiveEvent;
      hiveEvent = new AddPartitionEvent(addPartition.getTable(), addPartition.getPartitions(), addPartition.getStatus(),
          hmsHandlerFactory.newInstance());
      break;
    }
    case ON_ALTER_PARTITION: {
      ApiaryAlterPartitionEvent alterPartition = (ApiaryAlterPartitionEvent) serializableHiveEvent;
      hiveEvent = new AlterPartitionEvent(alterPartition.getOldPartition(), alterPartition.getNewPartition(),
          alterPartition.getTable(), alterPartition.getStatus(), hmsHandlerFactory.newInstance());
      break;
    }
    case ON_DROP_PARTITION: {
      ApiaryDropPartitionEvent dropPartition = (ApiaryDropPartitionEvent) serializableHiveEvent;
      hiveEvent = new DropPartitionEvent(dropPartition.getTable(), dropPartition.getPartitions().get(0),
          dropPartition.getStatus(), dropPartition.getDeleteData(), hmsHandlerFactory.newInstance());
      break;
    }
    case ON_CREATE_TABLE: {
      ApiaryCreateTableEvent createTableEvent = (ApiaryCreateTableEvent) serializableHiveEvent;
      hiveEvent = new CreateTableEvent(createTableEvent.getTable(), createTableEvent.getStatus(),
          hmsHandlerFactory.newInstance());
      break;
    }
    case ON_ALTER_TABLE: {
      ApiaryAlterTableEvent alterTableEvent = (ApiaryAlterTableEvent) serializableHiveEvent;
      hiveEvent = new AlterTableEvent(alterTableEvent.getOldTable(), alterTableEvent.getNewTable(),
          alterTableEvent.getStatus(), hmsHandlerFactory.newInstance());
      break;
    }
    case ON_DROP_TABLE: {
      ApiaryDropTableEvent dropTable = (ApiaryDropTableEvent) serializableHiveEvent;
      hiveEvent = new DropTableEvent(dropTable.getTable(), dropTable.getStatus(), dropTable.getDeleteData(),
          hmsHandlerFactory.newInstance());
      break;
    }

    case ON_INSERT: {
      ApiaryInsertEvent insert = (ApiaryInsertEvent) serializableHiveEvent;

      List<String> partVals = new ArrayList<>();
      Map<String, String> keyValues = insert.getPartitionKeyValues();

      for (String value : keyValues.values()) {
        partVals.add(value);
      }

      InsertEventRequestData insertEventRequestData = new InsertEventRequestData(insert.getFiles());
      insertEventRequestData.setFilesAddedChecksum(insert.getFileChecksums());

      hiveEvent = new InsertEvent(insert.getDatabaseName(), insert.getTableName(), partVals, insertEventRequestData,
          insert.getStatus(), hmsHandlerFactory.newInstance());
      break;
    }
    default:
      throw new DroneFlyException("Unsupported event type: " + serializableHiveEvent.getEventType().toString());
    }

    return hiveEvent;

  }
}
