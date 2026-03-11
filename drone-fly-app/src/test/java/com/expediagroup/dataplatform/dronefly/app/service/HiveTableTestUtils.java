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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveTableTestUtils {

  private HiveTableTestUtils() {}

  public static final List<FieldSchema> PARTITION_COLUMNS = Arrays
      .asList(new FieldSchema("partition1", "string", ""), new FieldSchema("partition2", "string", ""));

  public static Table createPartitionedTable(String database, String table, String location) {
    Table hiveTable = new Table();
    hiveTable.setDbName(database);
    hiveTable.setTableName(table);
    hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
    hiveTable.putToParameters("EXTERNAL", "TRUE");

    hiveTable.setPartitionKeys(PARTITION_COLUMNS);

    List<FieldSchema> columns = new ArrayList<FieldSchema>();
    columns.add(new FieldSchema("test_col1", "string", ""));
    columns.add(new FieldSchema("test_col2", "string", ""));
    columns.add(new FieldSchema("test_col3", "string", ""));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(columns);
    sd.setLocation(location);
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());

    hiveTable.setSd(sd);

    return hiveTable;
  }

  public static Partition newPartition(Table hiveTable, List<String> values, String location) {
    Partition partition = new Partition();
    partition.setDbName(hiveTable.getDbName());
    partition.setTableName(hiveTable.getTableName());
    partition.setValues(values);
    partition.setSd(new StorageDescriptor(hiveTable.getSd()));
    partition.getSd().setLocation(location);
    return partition;
  }

}
