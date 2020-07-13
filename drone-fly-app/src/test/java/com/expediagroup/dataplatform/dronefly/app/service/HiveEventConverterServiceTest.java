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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryAddPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryAlterPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryAlterTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryCreateTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryDropPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryDropTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryInsertEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryListenerEventFactory;
import com.expediagroup.dataplatform.dronefly.app.service.factory.HMSHandlerFactory;

public class HiveEventConverterServiceTest {

  private static final String APP_NAME = "drone-fly";
  private static final String DB_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final String TABLE_LOCATION = "s3://test_location/";
  private static final String OLD_TABLE_LOCATION = "s3://old_test_location";
  private static final List<String> PARTITION_VALUES = Arrays.asList("p1", "p2");
  private static final String PARTITION_LOCATION = "s3://test_location/partition";
  private static final String OLD_PARTITION_LOCATION = "s3://old_partition_test_location";

  private final ApiaryListenerEventFactory apiaryListenerEventFactory = new ApiaryListenerEventFactory();
  private final Table hiveTable = HiveTableTestUtils.createPartitionedTable(DB_NAME, TABLE_NAME, TABLE_LOCATION);
  private final Partition partition = HiveTableTestUtils.newPartition(hiveTable, PARTITION_VALUES, PARTITION_LOCATION);
  private HMSHandler hmsHandler;

  private HiveEventConverterService hiveEventConverterService;

  @BeforeEach
  public void init() throws MetaException {
    hiveEventConverterService = new HiveEventConverterService(new HMSHandlerFactory(new HiveConf()));
  }

  @Test
  public void createTableEvent() throws MetaException, NoSuchObjectException {
    CreateTableEvent createTableEvent = createCreateTableEvent();
    ApiaryCreateTableEvent apiaryCreateTableEvent = apiaryListenerEventFactory.create(createTableEvent);
    CreateTableEvent result = (CreateTableEvent) hiveEventConverterService.toHiveEvent(apiaryCreateTableEvent);

    assertThat(result.getHandler().getName()).isEqualTo(APP_NAME);
    assertThat(result.getTable().getDbName()).isEqualTo(DB_NAME);
    assertThat(result.getTable().getTableName()).isEqualTo(TABLE_NAME);
    assertThat(result.getTable().getSd().getLocation()).isEqualTo(TABLE_LOCATION);
  }

  @Test
  public void dropTableEvent() throws MetaException, NoSuchObjectException {
    DropTableEvent dropTableEvent = createDropTableEvent();
    ApiaryDropTableEvent apiaryDropTableEvent = apiaryListenerEventFactory.create(dropTableEvent);
    DropTableEvent result = (DropTableEvent) hiveEventConverterService.toHiveEvent(apiaryDropTableEvent);

    assertThat(result.getHandler().getName()).isEqualTo(APP_NAME);
    assertThat(result.getTable().getDbName()).isEqualTo(DB_NAME);
    assertThat(result.getTable().getTableName()).isEqualTo(TABLE_NAME);
    assertThat(result.getTable().getSd().getLocation()).isEqualTo(TABLE_LOCATION);
  }

  @Test
  public void alterTableEvent() throws MetaException, NoSuchObjectException {
    AlterTableEvent alterTableEvent = createAlterTableEvent();
    ApiaryAlterTableEvent apiaryAlterTableEvent = apiaryListenerEventFactory.create(alterTableEvent);
    AlterTableEvent result = (AlterTableEvent) hiveEventConverterService.toHiveEvent(apiaryAlterTableEvent);

    assertThat(result.getHandler().getName()).isEqualTo(APP_NAME);
    assertThat(result.getNewTable().getDbName()).isEqualTo(DB_NAME);
    assertThat(result.getNewTable().getTableName()).isEqualTo(TABLE_NAME);
    assertThat(result.getNewTable().getSd().getLocation()).isEqualTo(TABLE_LOCATION);

    assertThat(result.getOldTable().getDbName()).isEqualTo(DB_NAME);
    assertThat(result.getOldTable().getTableName()).isEqualTo(TABLE_NAME);
    assertThat(result.getOldTable().getSd().getLocation()).isEqualTo(OLD_TABLE_LOCATION);
  }

  @Test
  public void addPartitionEvent() throws MetaException, NoSuchObjectException {
    AddPartitionEvent addPartitionEvent = createAddPartitionEvent();
    ApiaryAddPartitionEvent apiaryAddPartitionEvent = apiaryListenerEventFactory.create(addPartitionEvent);
    AddPartitionEvent result = (AddPartitionEvent) hiveEventConverterService.toHiveEvent(apiaryAddPartitionEvent);

    assertThat(result.getHandler().getName()).isEqualTo(APP_NAME);
    assertThat(result.getTable().getDbName()).isEqualTo(DB_NAME);
    assertThat(result.getTable().getTableName()).isEqualTo(TABLE_NAME);
    assertThat(result.getTable().getSd().getLocation()).isEqualTo(TABLE_LOCATION);

    Partition partitionResult = result.getPartitionIterator().next();
    assertThat(partitionResult.getValues()).isEqualTo(PARTITION_VALUES);
    assertThat(partitionResult.getSd().getLocation()).isEqualTo(PARTITION_LOCATION);
  }

  @Test
  public void dropPartitionEvent() throws MetaException, NoSuchObjectException {
    DropPartitionEvent DropPartitionEvent = createDropPartitionEvent();
    ApiaryDropPartitionEvent apiaryDropPartitionEvent = apiaryListenerEventFactory.create(DropPartitionEvent);
    DropPartitionEvent result = (DropPartitionEvent) hiveEventConverterService.toHiveEvent(apiaryDropPartitionEvent);

    assertThat(result.getHandler().getName()).isEqualTo(APP_NAME);
    assertThat(result.getTable().getDbName()).isEqualTo(DB_NAME);
    assertThat(result.getTable().getTableName()).isEqualTo(TABLE_NAME);
    assertThat(result.getTable().getSd().getLocation()).isEqualTo(TABLE_LOCATION);

    Partition partitionResult = result.getPartitionIterator().next();
    assertThat(partitionResult.getValues()).isEqualTo(PARTITION_VALUES);
    assertThat(partitionResult.getSd().getLocation()).isEqualTo(PARTITION_LOCATION);
  }

  @Test
  public void alterPartitionEvent() throws MetaException, NoSuchObjectException {
    AlterPartitionEvent AlterPartitionEvent = createAlterPartitionEvent();
    ApiaryAlterPartitionEvent apiaryAlterPartitionEvent = apiaryListenerEventFactory.create(AlterPartitionEvent);
    AlterPartitionEvent result = (AlterPartitionEvent) hiveEventConverterService.toHiveEvent(apiaryAlterPartitionEvent);

    assertThat(result.getHandler().getName()).isEqualTo(APP_NAME);
    assertThat(result.getTable().getDbName()).isEqualTo(DB_NAME);
    assertThat(result.getTable().getTableName()).isEqualTo(TABLE_NAME);
    assertThat(result.getTable().getSd().getLocation()).isEqualTo(TABLE_LOCATION);

    Partition newPartitionResult = result.getNewPartition();
    assertThat(newPartitionResult.getValues()).isEqualTo(PARTITION_VALUES);
    assertThat(newPartitionResult.getSd().getLocation()).isEqualTo(PARTITION_LOCATION);

    Partition oldPartitionResult = result.getOldPartition();
    assertThat(oldPartitionResult.getValues()).isEqualTo(PARTITION_VALUES);
    assertThat(oldPartitionResult.getSd().getLocation()).isEqualTo(OLD_PARTITION_LOCATION);
  }

  @Test
  public void insertEvent() throws MetaException, NoSuchObjectException {
    // Mocking here is necessary because of handler.get_table_req(req).getTable() call in InsertEvent constructor.
    HMSHandlerFactory hmsHandlerFactory = mock(HMSHandlerFactory.class);
    HMSHandler mockHmsHandler = mock(HMSHandler.class);
    GetTableResult gtr = mock(GetTableResult.class);
    when(mockHmsHandler.getName()).thenReturn(APP_NAME);
    when(mockHmsHandler.get_table_req(any())).thenReturn(gtr);
    when(gtr.getTable()).thenReturn(hiveTable);
    when(hmsHandlerFactory.newInstance()).thenReturn(mockHmsHandler);

    hiveEventConverterService = new HiveEventConverterService(hmsHandlerFactory);

    InsertEvent InsertEvent = createInsertEvent(hmsHandlerFactory);
    ApiaryInsertEvent apiaryInsertEvent = apiaryListenerEventFactory.create(InsertEvent);
    InsertEvent result = (InsertEvent) hiveEventConverterService.toHiveEvent(apiaryInsertEvent);

    assertThat(result.getHandler().getName()).isEqualTo(APP_NAME);
    assertThat(result.getDb()).isEqualTo(DB_NAME);
    assertThat(result.getTable()).isEqualTo(TABLE_NAME);
    assertThat(result.getFiles()).isEqualTo(Arrays.asList("file:/a/b.txt", "file:/a/c.txt"));
    assertThat(result.getFileChecksums()).isEqualTo(Arrays.asList("123", "456"));
  }

  @Test
  public void nullEvent() throws MetaException, NoSuchObjectException {
    CreateTableEvent result = (CreateTableEvent) hiveEventConverterService.toHiveEvent(null);
    assertThat(result).isNull();
  }

  private InsertEvent createInsertEvent(HMSHandlerFactory hmsHandlerFactory)
    throws MetaException, NoSuchObjectException {
    List<String> files = Arrays.asList("file:/a/b.txt", "file:/a/c.txt");
    List<String> fileChecksums = Arrays.asList("123", "456");
    InsertEventRequestData insertRequestData = new InsertEventRequestData(files);
    insertRequestData.setFilesAddedChecksum(fileChecksums);
    InsertEvent event = new InsertEvent(DB_NAME, TABLE_NAME, PARTITION_VALUES, insertRequestData, true,
        hmsHandlerFactory.newInstance());
    return event;
  }

  private AddPartitionEvent createAddPartitionEvent() throws MetaException {
    AddPartitionEvent event = new AddPartitionEvent(hiveTable, partition, true, hmsHandler);
    return event;
  }

  private AlterPartitionEvent createAlterPartitionEvent() throws MetaException {
    Partition oldPartition = HiveTableTestUtils.newPartition(hiveTable, PARTITION_VALUES, OLD_PARTITION_LOCATION);
    AlterPartitionEvent event = new AlterPartitionEvent(oldPartition, partition, hiveTable, true, hmsHandler);
    return event;
  }

  private DropPartitionEvent createDropPartitionEvent() throws MetaException {
    DropPartitionEvent event = new DropPartitionEvent(hiveTable, partition, true, false, hmsHandler);
    return event;
  }

  private CreateTableEvent createCreateTableEvent() throws MetaException {
    CreateTableEvent event = new CreateTableEvent(hiveTable, true, hmsHandler);
    return event;
  }

  private AlterTableEvent createAlterTableEvent() throws MetaException {
    Table oldTable = HiveTableTestUtils.createPartitionedTable(DB_NAME, TABLE_NAME, OLD_TABLE_LOCATION);
    AlterTableEvent event = new AlterTableEvent(oldTable, hiveTable, true, hmsHandler);
    return event;
  }

  private DropTableEvent createDropTableEvent() throws MetaException {
    DropTableEvent event = new DropTableEvent(hiveTable, true, false, hmsHandler);
    return event;
  }

}
