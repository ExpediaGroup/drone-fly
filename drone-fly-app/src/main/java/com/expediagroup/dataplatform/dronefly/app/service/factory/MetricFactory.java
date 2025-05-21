package com.expediagroup.dataplatform.dronefly.app.service.factory;

import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricFactory {

  private static final Logger log = LoggerFactory.getLogger(MetricFactory.class);

  private final HiveConf hiveConf;

  public MetricFactory(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public void init() {
    try {
      MetricsFactory.init(hiveConf);
      log.error("Hive metrics initialized");
    } catch (Exception e) {
      log.error("Metrics could not be init", e);
    }
  }
}
