/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
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

import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.*;

/**
 * This class detects disk failures. This class will be scheduled to run periodically to check if there is bad disk on
 * any alive broker in the cluster.
 **/
public class DiskFailureDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DiskFailureDetector.class);
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final AdminClient _adminClient;
  private final LoadMonitor _loadMonitor;
  private final Queue<Anomaly> _anomalies;
  private final Time _time;
  private final boolean _allowCapacityEstimation;
  private int _lastCheckedClusterGeneration;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private static final long LOGDIR_RESPONSE_TIMEOUT_MS = 10000;
  private static final long MAX_METADATA_WAIT_MS = 60000L;

  public DiskFailureDetector(KafkaCruiseControlConfig config,
                             LoadMonitor loadMonitor,
                             AdminClient adminClient,
                             Queue<Anomaly> anomalies,
                             Time time,
                             KafkaCruiseControl kafkaCruiseControl) {
    _loadMonitor = loadMonitor;
    _adminClient = adminClient;
    _anomalies = anomalies;
    _time = time;
    _lastCheckedClusterGeneration = -1;
    _kafkaCruiseControl = kafkaCruiseControl;
    _allowCapacityEstimation = config.getBoolean(KafkaCruiseControlConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    _excludeRecentlyDemotedBrokers = config.getBoolean(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    _excludeRecentlyRemovedBrokers = config.getBoolean(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
  }

  /**
   * Skip bad disk detection if any of the following is true:
   * <ul>
   * <li>Cluster model generation has not changed since the last bad disk check.</li>
   * <li>There is dead broker in the cluster, {@link BrokerFailureDetector} should take care of the anomaly.</li>
   * <li>Load monitor is not ready.</li>
   * <li>There is an ongoing execution.</li>
   * </ul>
   *
   * @return True to skip bad disk detection based on the current state, false otherwise.
   */
  private boolean shouldSkipBadDiskDetection() {
    int currentClusterGeneration =  _loadMonitor.clusterModelGeneration().clusterGeneration();
    if (currentClusterGeneration == _lastCheckedClusterGeneration) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping bad disk detection because the model generation hasn't changed. Current model generation {}",
                  _loadMonitor.clusterModelGeneration());
      }
      return true;
    }
    _lastCheckedClusterGeneration = currentClusterGeneration;

    Set<Integer> deadBrokers = _loadMonitor.deadBrokers(MAX_METADATA_WAIT_MS);
    if (!deadBrokers.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping goal violation detection because there are dead broker in the cluster, dead broker: {}",
                  deadBrokers);
      }
      return true;
    }

    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();
    if (!ViolationUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
      LOG.info("Skipping bad disk detection because load monitor is in {} state.", loadMonitorTaskRunnerState);
      return true;
    }

    ExecutorState.State executorState = _kafkaCruiseControl.state(
        new OperationProgress(), Collections.singleton(EXECUTOR)).executorState().state();
    if (executorState != ExecutorState.State.NO_TASK_IN_PROGRESS) {
      LOG.info("Skipping bad disk detection because the executor is in {} state.", executorState);
      return true;
    }

    return false;
  }

  @Override
  public void run() {
    if (shouldSkipBadDiskDetection()) {
      return;
    }

    Map<Integer, Map<String, Long>> failedDisksByBroker = new HashMap<>();
    Set<Integer> aliveBrokers = _loadMonitor.kafkaCluster().nodes().stream().mapToInt(Node::id).boxed().collect(Collectors.toSet());
    _adminClient.describeLogDirs(aliveBrokers).values().forEach((broker, future) -> {
      try {
      future.get(LOGDIR_RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS).forEach((logdir, info) -> {
        if (info.error != Errors.NONE) {
              failedDisksByBroker.putIfAbsent(broker, new HashMap<>());
              failedDisksByBroker.get(broker).put(logdir, _time.milliseconds());
            }
          });
        } catch (TimeoutException te) {
          LOG.warn("Getting logdir information for broker {} timed out.", broker);
        } catch (InterruptedException | ExecutionException e) {
        LOG.warn("Getting logdir information for broker {} encounter exception {}.", broker, e);
      }
    });
    if (!failedDisksByBroker.isEmpty()) {
      _anomalies.add(new DiskFailures(_kafkaCruiseControl,
                                      failedDisksByBroker,
                                      _allowCapacityEstimation,
                                      _excludeRecentlyDemotedBrokers,
                                      _excludeRecentlyRemovedBrokers));
    }
    LOG.debug("Disk failure detection finished.");
  }
}