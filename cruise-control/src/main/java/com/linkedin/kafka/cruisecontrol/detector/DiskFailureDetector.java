/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.DISK_FAILURES_CLASS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.shouldSkipAnomalyDetection;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;

/**
 * This class detects disk failures.
 **/
public class DiskFailureDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DiskFailureDetector.class);
  public static final String FAILED_DISKS_CONFIG = "failed.disks";
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final AdminClient _adminClient;
  private final LoadMonitor _loadMonitor;
  private final Queue<KafkaAnomaly> _anomalies;
  private final Time _time;
  private int _lastCheckedClusterGeneration;
  private final KafkaCruiseControlConfig _config;

  public DiskFailureDetector(LoadMonitor loadMonitor,
                             AdminClient adminClient,
                             Queue<KafkaAnomaly> anomalies,
                             Time time,
                             KafkaCruiseControl kafkaCruiseControl) {
    _loadMonitor = loadMonitor;
    _adminClient = adminClient;
    _anomalies = anomalies;
    _time = time;
    _lastCheckedClusterGeneration = -1;
    _kafkaCruiseControl = kafkaCruiseControl;
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    _config = config;
  }

  /**
   * Skip disk failure detection if any of the following is true:
   * <ul>
   * <li>Cluster model generation has not changed since the last disk failure check.</li>
   * <li>There are dead brokers in the cluster, {@link BrokerFailureDetector} should take care of the anomaly.</li>
   * <li>{@link AnomalyDetectorUtils#shouldSkipAnomalyDetection(LoadMonitor, KafkaCruiseControl)} returns true.
   * </ul>
   *
   * @return True to skip disk failure detection based on the current state, false otherwise.
   */
  private boolean shouldSkipDiskFailureDetection() {
    int currentClusterGeneration = _loadMonitor.clusterModelGeneration().clusterGeneration();
    if (currentClusterGeneration == _lastCheckedClusterGeneration) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping disk failure detection because the model generation hasn't changed. Current model generation {}",
                  _loadMonitor.clusterModelGeneration());
      }
      return true;
    }
    _lastCheckedClusterGeneration = currentClusterGeneration;

    Set<Integer> deadBrokers = _loadMonitor.deadBrokersWithReplicas(MAX_METADATA_WAIT_MS);
    if (!deadBrokers.isEmpty()) {
      LOG.debug("Skipping disk failure detection because there are dead broker in the cluster, dead broker: {}", deadBrokers);
      return true;
    }

    return shouldSkipAnomalyDetection(_loadMonitor, _kafkaCruiseControl);
  }

  @Override
  public void run() {
    try {
      if (shouldSkipDiskFailureDetection()) {
        return;
      }
      Map<Integer, Map<String, Long>> failedDisksByBroker = new HashMap<>();
      Set<Integer> aliveBrokers = _loadMonitor.kafkaCluster().nodes().stream().mapToInt(Node::id).boxed().collect(Collectors.toSet());
      _adminClient.describeLogDirs(aliveBrokers).values().forEach((broker, future) -> {
        try {
          future.get(_config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS).forEach((logdir, info) -> {
            if (info.error != Errors.NONE) {
              failedDisksByBroker.putIfAbsent(broker, new HashMap<>());
              failedDisksByBroker.get(broker).put(logdir, _time.milliseconds());
            }
          });
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
          LOG.warn("Retrieving logdir information for broker {} encountered exception {}.", broker, e);
        }
      });
      if (!failedDisksByBroker.isEmpty()) {
        Map<String, Object> parameterConfigOverrides = new HashMap<>(3);
        parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
        parameterConfigOverrides.put(FAILED_DISKS_CONFIG, failedDisksByBroker);
        parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_CONFIG, _time.milliseconds());
        DiskFailures diskFailures = _config.getConfiguredInstance(DISK_FAILURES_CLASS_CONFIG,
                                                                  DiskFailures.class,
                                                                  parameterConfigOverrides);
        _anomalies.add(diskFailures);
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    } finally {
      LOG.debug("Disk failure detection finished.");
    }
  }
}