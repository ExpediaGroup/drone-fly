package com.expediagroup.dataplatform.dronefly.app.metrics;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Timer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

public class DropwizardToMicrometerBridge implements MetricRegistryListener {

  private final MeterRegistry meterRegistry;

  public DropwizardToMicrometerBridge(MeterRegistry meterRegistry, MetricRegistry metricRegistry) {
    this.meterRegistry = meterRegistry;
    metricRegistry.addListener(this);
  }

  @Override
  public void onGaugeAdded(String name, Gauge<?> gauge) {
    meterRegistry.gauge(name, gauge, DropwizardToMicrometerBridge::doubleValue);
  }

  @Override
  public void onGaugeRemoved(String name) {
    throw new RuntimeException("Method not implemented");
  }

  @Override
  public void onCounterAdded(String name, Counter counter) {
    meterRegistry.more().counter(name, Tags.empty(), counter, Counter::getCount);
  }

  @Override
  public void onCounterRemoved(String name) {
    throw new RuntimeException("Method not implemented");
  }

  @Override
  public void onHistogramAdded(String name, Histogram histogram) {
    meterRegistry.more()
        .timer(name, Tags.empty(), histogram, Histogram::getCount, DropwizardToMicrometerBridge::totalTime,
            TimeUnit.MILLISECONDS);
  }

  @Override
  public void onHistogramRemoved(String name) {
    throw new RuntimeException("Method not implemented");
  }

  @Override
  public void onMeterAdded(String name, Meter meter) {
    meterRegistry.more().counter(name, Tags.empty(), meter, Meter::getCount);
  }

  @Override
  public void onMeterRemoved(String name) {
    throw new RuntimeException("Method not implemented");
  }

  @Override
  public void onTimerAdded(String name, Timer timer) {
    meterRegistry.more()
        .timer(name, Tags.empty(), timer, Timer::getCount, DropwizardToMicrometerBridge::totalTime,
            TimeUnit.MILLISECONDS);
  }

  @Override
  public void onTimerRemoved(String name) {
    throw new RuntimeException("Method not implemented");
  }

  public static long totalTime(Sampling sampling) {
    return Arrays.stream(sampling.getSnapshot().getValues()).sum();
  }

  public static double doubleValue(Gauge<?> gauge) {
    if (gauge == null || gauge.getValue() == null) {
      return Double.NaN;
    }
    Object value = gauge.getValue();
    return Double.valueOf(value.toString());
  }
}
