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

public class DefaultDroneFlyListener extends MetaStoreEventListener {
  private static final Logger log = LoggerFactory.getLogger(DefaultDroneFlyListener.class);

  public DefaultDroneFlyListener(Configuration config) {
    super(config);
  }

  @Override
  public void onAddPartition(AddPartitionEvent addPartitionEvent) throws MetaException {
    if (!addPartitionEvent.getStatus()) {
      return;
    }
    log.info("Event Type: {}", EventType.ADD_PARTITION.toString());
    log.info("DB Name: {}", addPartitionEvent.getTable().getDbName());
    log.info("Table Name: {}", addPartitionEvent.getTable().getTableName());
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent createDatabaseEvent) throws MetaException {
    if (!createDatabaseEvent.getStatus()) {
      return;
    }
    log.info("Event Type: {}", EventType.CREATE_DATABASE.toString());
    log.info("DB Name: {}", createDatabaseEvent.getDatabase().getName());
    log.info("DB Location: {}", createDatabaseEvent.getDatabase().getLocationUri());
  }

  @Override
  public void onCreateTable(CreateTableEvent createTableEvent) throws MetaException {
    if (!createTableEvent.getStatus()) {
      return;
    }
    log.info("Event Type: {}", EventType.CREATE_TABLE.toString());
    log.info("DB Name: {}", createTableEvent.getTable().getDbName());
    log.info("Table Name: {}", createTableEvent.getTable().getTableName());
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dropDatabaseEvent) throws MetaException {
    if (!dropDatabaseEvent.getStatus()) {
      return;
    }
    log.info("Event Type: {}", EventType.DROP_DATABASE.toString());
    log.info("DB Name: {}", dropDatabaseEvent.getDatabase().getName());
    log.info("DB Location: {}", dropDatabaseEvent.getDatabase().getLocationUri());
  }

  @Override
  public void onDropPartition(DropPartitionEvent dropPartitionEvent) throws MetaException {
    if (!dropPartitionEvent.getStatus()) {
      return;
    }
    log.info("Event Type: {}", EventType.DROP_PARTITION.toString());
    log.info("DB Name: {}", dropPartitionEvent.getTable().getDbName());
    log.info("Table Name: {}", dropPartitionEvent.getTable().getTableName());
  }

  @Override
  public void onDropTable(DropTableEvent dropTableEvent) throws MetaException {
    if (!dropTableEvent.getStatus()) {
      return;
    }
    log.info("Event Type: {}", EventType.DROP_TABLE.toString());
    log.info("DB Name: {}", dropTableEvent.getTable().getDbName());
    log.info("Table Name: {}", dropTableEvent.getTable().getTableName());
  }

  @Override
  public void onAlterTable(AlterTableEvent alterTableEvent) throws MetaException {
    if (!alterTableEvent.getStatus()) {
      return;
    }
    log.info("Event Type: {}", EventType.ALTER_TABLE.toString());
    log.info("Old DB Name: {}", alterTableEvent.getOldTable().getDbName());
    log.info("Old Table Name: {}", alterTableEvent.getOldTable().getTableName());
    log.info("Old Table Location: {}", alterTableEvent.getOldTable().getSd().getLocation());
    log.info("New DB Name: {}", alterTableEvent.getNewTable().getDbName());
    log.info("New Table Name: {}", alterTableEvent.getNewTable().getTableName());
    log.info("Old Table Location: {}", alterTableEvent.getNewTable().getSd().getLocation());
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent alterPartitionEvent) throws MetaException {
    if (!alterPartitionEvent.getStatus()) {
      return;
    }
    log.info("Event Type: {}", EventType.ALTER_PARTITION.toString());
    log.info("DB Name: {}", alterPartitionEvent.getOldPartition().getDbName());
    log.info("Table Name: {}", alterPartitionEvent.getOldPartition().getTableName());
    log.info("Old partition Location: {}", alterPartitionEvent.getOldPartition().getSd().getLocation());
    log.info("New partition Location: {}", alterPartitionEvent.getNewPartition().getSd().getLocation());
  }

  @Override
  public void onCreateFunction(CreateFunctionEvent createFunctionEvent) throws MetaException {
    if (!createFunctionEvent.getStatus()) {
      return;
    }
    log.info("Event Type: {}", EventType.CREATE_FUNCTION.toString());
    log.info("Function Name: {}", createFunctionEvent.getFunction().getFunctionName());
    log.info("Function Class Name: {}", createFunctionEvent.getFunction().getClassName());
  }

  @Override
  public void onDropFunction(DropFunctionEvent dropFunctionEvent) throws MetaException {
    if (!dropFunctionEvent.getStatus()) {
      return;
    }
    log.info("Event Type: {}", EventType.DROP_FUNCTION.toString());
    log.info("Function Name: {}", dropFunctionEvent.getFunction().getClassName());
    log.info("Function Class Name: {}", dropFunctionEvent.getFunction().getClassName());
  }

}
