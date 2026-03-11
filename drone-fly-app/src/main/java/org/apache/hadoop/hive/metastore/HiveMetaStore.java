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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * Compatibility shim that re-introduces {@code HiveMetaStore.HMSHandler} as an inner class. In Hive
 * 4.x, {@code HMSHandler} became a standalone top-level class and the inner class was removed.
 * Libraries compiled against Hive 3.x (e.g. apiary-hive-events &lt;= 8.1.15) still reference {@code
 * HiveMetaStore$HMSHandler}, so this shim restores it.
 */
public class HiveMetaStore {

  public static class HMSHandler extends org.apache.hadoop.hive.metastore.HMSHandler {
    public HMSHandler(String name, Configuration conf) throws MetaException {
      super(name, conf);
    }
  }
}
