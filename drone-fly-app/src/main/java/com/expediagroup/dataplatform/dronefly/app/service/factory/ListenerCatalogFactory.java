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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.dataplatform.dronefly.app.service.ListenerCatalog;
import com.expediagroup.dataplatform.dronefly.app.service.listener.LoggingMetastoreListener;

public class ListenerCatalogFactory {
  private static final Logger log = LoggerFactory.getLogger(ListenerCatalogFactory.class);
  private final HiveConf hiveConf;

  public ListenerCatalogFactory(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public ListenerCatalog newInstance(String confProvidedList) {
    String listenerImplList = confProvidedList;
    if (StringUtils.isBlank(listenerImplList)) {
      log.info("{apiary.listener.list} is empty. Going to look in hive-site.xml if it is provided on the classpath.");
      listenerImplList = hiveConf.getVar(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS);
    }

    if (StringUtils.isBlank(listenerImplList)) {
      log
          .warn(
              "No Hive metastore listeners have been provided as argument {apiary.listener.list} or hive-site.xml. Going to use: {}",
              LoggingMetastoreListener.class.getName());
      listenerImplList = LoggingMetastoreListener.class.getName();
    }

    return new ListenerCatalog(hiveConf, listenerImplList);
  }

}
