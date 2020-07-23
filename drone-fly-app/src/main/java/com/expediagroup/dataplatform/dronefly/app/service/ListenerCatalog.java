/**
 * Copyright (C) 2009-2020 Expedia, Inc and Apache Hive contributors.
 *
 * Based on {@code org.apache.hadoop.hive.metastore.MetaStoreUtils} from hive-metastore 2.3.7:
 *
 * https://github.com/apache/hive/blob/rel/release-2.3.7/metastore/src/java/org/apache/hadoop/hive/metastore/MetaStoreUtils.java#L1642
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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;

import com.expediagroup.dataplatform.dronefly.core.exception.DroneFlyException;

public class ListenerCatalog {
  private final List<MetaStoreEventListener> listeners;

  public ListenerCatalog(HiveConf conf, String listenerImplList) {
    if (StringUtils.isBlank(listenerImplList)) {
      listenerImplList = "com.expediagroup.dataplatform.dronefly.app.service.listener.DefaultDroneFlyListener";
    }
    listeners = getMetaStoreListeners(MetaStoreEventListener.class, conf, listenerImplList.trim());
  }

  public List<MetaStoreEventListener> getListeners() {
    return listeners;
  }

  /**
   * create listener instances as per the configuration.
   *
   * @param clazz
   * @param conf
   * @param listenerImplList
   * @return
   * @throws DroneFlyException
   */
  private <T> List<T> getMetaStoreListeners(Class<T> clazz, HiveConf conf, String listenerImplList)
    throws DroneFlyException {

    List<T> listeners = new ArrayList<T>();

    String[] listenerImpls = listenerImplList.split(",");
    for (String listenerImpl : listenerImpls) {
      try {
        T listener = (T) Class
            .forName(listenerImpl.trim(), true, JavaUtils.getClassLoader())
            .getConstructor(Configuration.class)
            .newInstance(conf);
        listeners.add(listener);
      } catch (Exception e) {
        throw new DroneFlyException("Failed to instantiate listener named: " + listenerImpl + ", reason: ", e);
      }
    }

    return listeners;
  }

}
