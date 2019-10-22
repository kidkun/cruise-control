/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_DESCRIPTION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_BROKER_ENTITY_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_METRIC_ID_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_TIME_WINDOW_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType.METRIC_ANOMALY;


/**
 * A class that holds Kafka metric anomalies.
 * A Kafka metric anomaly indicates unexpected rapid changes in metric values of a broker.
 */
public class KafkaMetricAnomaly extends KafkaAnomaly implements MetricAnomaly<BrokerEntity> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricAnomaly.class);
  protected static final String ID_PREFIX = METRIC_ANOMALY.toString();
  protected String _description;
  protected BrokerEntity _brokerEntity;
  protected Short _metricId;
  protected List<Long> _windows;
  protected String _anomalyId;

  public KafkaMetricAnomaly() {
  }

  /**
   * Get a list of windows for which a metric anomaly was observed.
   */
  @Override
  public List<Long> windows() {
    return _windows;
  }

  /**
   * Get the anomaly description.
   */
  @Override
  public String description() {
    return _description;
  }

  /**
   * Get the broker entity with metric anomaly.
   */
  @Override
  public BrokerEntity entity() {
    return _brokerEntity;
  }

  /**
   * Get the metric Id caused the metric anomaly.
   */
  @Override
  public Short metricId() {
    return _metricId;
  }

  @Override
  public String anomalyId() {
    return _anomalyId;
  }

  /**
   * Fix the anomaly with the system.
   */
  @Override
  public boolean fix() throws KafkaCruiseControlException {
    // TODO: Fix the cluster by removing the leadership from the brokers with metric anomaly (See PR#175: demote_broker).
    LOG.trace("Fix the cluster by removing the leadership from the broker: {}", _brokerEntity);
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s anomaly with id: %s Metric Anomaly windows: %s description: %s",
                         METRIC_ANOMALY, anomalyId(), _windows, _description);
  }

  /**
   * Get the optimization result of self healing process, or null if no optimization result is available.
   *
   * @param isJson True for JSON response, false otherwise.
   * @return the optimization result of self healing process, or null if no optimization result is available.
   */
  String optimizationResult(boolean isJson) {
    if (_optimizationResult == null) {
      return null;
    }
    return isJson ? _optimizationResult.cachedJSONResponse() : _optimizationResult.cachedPlaintextResponse();
  }

  @Override
  public AnomalyType anomalyType() {
    return METRIC_ANOMALY;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _anomalyId = String.format("%s-%s", ID_PREFIX, UUID.randomUUID().toString().substring(ID_PREFIX.length() + 1));
    _description = (String) configs.get(METRIC_ANOMALY_DESCRIPTION_CONFIG);
    _brokerEntity = (BrokerEntity) configs.get(METRIC_ANOMALY_BROKER_ENTITY_CONFIG);
    _metricId = (Short) configs.get(METRIC_ANOMALY_METRIC_ID_CONFIG);
    _windows = (List<Long>) configs.get(METRIC_ANOMALY_TIME_WINDOW_CONFIG);
    _optimizationResult = null;
  }
}
