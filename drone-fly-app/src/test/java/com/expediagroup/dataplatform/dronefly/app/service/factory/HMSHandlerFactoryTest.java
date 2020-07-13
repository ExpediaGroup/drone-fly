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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.jupiter.api.Test;


public class HMSHandlerFactoryTest {
  private HMSHandlerFactory factory;

  @Test
  public void typical() throws MetaException {
    HiveConf conf = new HiveConf();
    conf.set("test-property", "test");
    factory = new HMSHandlerFactory(conf);

    HMSHandler hmsHandler = factory.newInstance();

    assertThat(hmsHandler.getName()).isEqualTo("drone-fly");
    assertThat(hmsHandler.getHiveConf().get("test-property")).isEqualTo("test");
  }

}
