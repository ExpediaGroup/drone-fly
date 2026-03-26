/**
 * Copyright (C) 2020-2026 Expedia, Inc.
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
package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Compatibility shim for {@code CreateTableEvent}. Provides both the Hive 3.x 3-argument
 * constructor (required by libraries compiled against Hive 3.x, e.g. apiary-hive-events &lt;=
 * 8.1.15) and the Hive 4.x 4-argument constructor used by the rest of this application.
 *
 * <p>This class shadows the Hive jar's {@code CreateTableEvent} because it appears earlier on the
 * classpath (compiled into {@code target/classes}), resolving the {@code NoSuchMethodError} thrown
 * by {@code JsonMetaStoreEventSerDe$HeplerApiaryListenerEvent} at runtime.
 */
public class CreateTableEvent extends ListenerEvent {

  private final Table table;
  private final boolean isReplicated;

  /**
   * Compatibility constructor for libraries compiled against Hive 3.x (e.g. apiary-hive-events
   * &lt;= 8.1.15 / {@code JsonMetaStoreEventSerDe}).
   */
  public CreateTableEvent(Table table, boolean status, HiveMetaStore.HMSHandler handler) {
    super(status, handler);
    this.table = table;
    this.isReplicated = false;
  }

  /** Hive 4.x constructor. */
  public CreateTableEvent(Table table, boolean status, IHMSHandler handler, boolean isReplicated) {
    super(status, handler);
    this.table = table;
    this.isReplicated = isReplicated;
  }

  public Table getTable() {
    return table;
  }

  public boolean isReplicated() {
    return isReplicated;
  }
}
