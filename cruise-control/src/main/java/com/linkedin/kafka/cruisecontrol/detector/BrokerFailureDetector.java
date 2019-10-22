/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import kafka.cluster.Broker;
import kafka.zk.BrokerIdsZNode;
import kafka.zk.KafkaZkClient;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.ZK_SESSION_TIMEOUT;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.ZK_CONNECTION_TIMEOUT;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static java.util.stream.Collectors.toSet;


/**
 * This class detects broker failures.
 */
public class BrokerFailureDetector {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerFailureDetector.class);
  public static final String FAILED_BROKERS_OBJECT_CONFIG = "failed.brokers.object";
  private static final String ZK_BROKER_FAILURE_METRIC_GROUP = "CruiseControlAnomaly";
  private static final String ZK_BROKER_FAILURE_METRIC_TYPE = "BrokerFailure";
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final String _failedBrokersZkPath;
  private final ZkClient _zkClient;
  private final KafkaZkClient _kafkaZkClient;
  private final Map<Integer, Long> _failedBrokers;
  private final LoadMonitor _loadMonitor;
  private final Queue<KafkaAnomaly> _anomalies;
  private final Time _time;

  public BrokerFailureDetector(LoadMonitor loadMonitor,
                               Queue<KafkaAnomaly> anomalies,
                               Time time,
                               KafkaCruiseControl kafkaCruiseControl) {
    KafkaCruiseControlConfig config = kafkaCruiseControl.config();
    String zkUrl = config.getString(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
    boolean zkSecurityEnabled = config.getBoolean(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    ZkConnection zkConnection = new ZkConnection(zkUrl, ZK_SESSION_TIMEOUT);
    _zkClient = new ZkClient(zkConnection, ZK_CONNECTION_TIMEOUT, new ZkStringSerializer());
    // Do not support secure ZK at this point.
    _kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zkUrl, ZK_BROKER_FAILURE_METRIC_GROUP, ZK_BROKER_FAILURE_METRIC_TYPE,
                                                                 zkSecurityEnabled);
    _failedBrokers = new HashMap<>();
    _failedBrokersZkPath = config.getString(KafkaCruiseControlConfig.FAILED_BROKERS_ZK_PATH_CONFIG);
    _loadMonitor = loadMonitor;
    _anomalies = anomalies;
    _time = time;
    _kafkaCruiseControl = kafkaCruiseControl;
  }

  void startDetection() {
    try {
      _zkClient.createPersistent(_failedBrokersZkPath);
    } catch (ZkNodeExistsException znee) {
      // let it go.
    }
    // Load the failed broker information from zookeeper.
    loadPersistedFailedBrokerList();
    // Detect broker failures.
    detectBrokerFailures();
    _zkClient.subscribeChildChanges(BrokerIdsZNode.path(), new BrokerFailureListener());
  }

  private synchronized void detectBrokerFailures(Set<Integer> aliveBrokers) {
    // update the failed broker information based on the current state.
    boolean updated = updateFailedBrokers(aliveBrokers);
    if (updated) {
      // persist the updated failed broker list.
      persistFailedBrokerList();
    }
    // Report the failures to anomaly detector to handle.
    reportBrokerFailures();
  }

  synchronized void detectBrokerFailures() {
    detectBrokerFailures(aliveBrokers());
  }

  synchronized Map<Integer, Long> failedBrokers() {
    return new HashMap<>(_failedBrokers);
  }

  void shutdown() {
    _zkClient.close();
    KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(_kafkaZkClient);
  }

  private void persistFailedBrokerList() {
    _zkClient.writeData(_failedBrokersZkPath, failedBrokerString());
  }

  private void loadPersistedFailedBrokerList() {
    String failedBrokerListString = _zkClient.readData(_failedBrokersZkPath);
    parsePersistedFailedBrokers(failedBrokerListString);
  }

  /**
   * If {@link #_failedBrokers} has changed, update it.
   *
   * @param aliveBrokers Alive brokers in the cluster.
   * @return true if {@link #_failedBrokers} has been updated, false otherwise.
   */
  private boolean updateFailedBrokers(Set<Integer> aliveBrokers) {
    // We get the complete broker list from metadata. i.e. any broker that still has a partition assigned to it is
    // included in the broker list. If we cannot update metadata in 60 seconds, skip
    Set<Integer> currentFailedBrokers = _loadMonitor.brokersWithReplicas(MAX_METADATA_WAIT_MS);
    currentFailedBrokers.removeAll(aliveBrokers);
    LOG.debug("Alive brokers: {}, failed brokers: {}", aliveBrokers, currentFailedBrokers);
    // Remove broker that is no longer failed.
    boolean updated = _failedBrokers.entrySet().removeIf(entry -> !currentFailedBrokers.contains(entry.getKey()));
    // Add broker that has just failed.
    for (Integer brokerId : currentFailedBrokers) {
      if (_failedBrokers.putIfAbsent(brokerId, _time.milliseconds()) == null) {
        updated = true;
      }
    }
    return updated;
  }

  private Set<Integer> aliveBrokers() {
    // We get the alive brokers from ZK directly.
    return JavaConversions.asJavaCollection(_kafkaZkClient.getAllBrokersInCluster())
                          .stream().map(Broker::id).collect(toSet());
  }

  private String failedBrokerString() {
    StringBuilder sb = new StringBuilder();
    for (Iterator<Map.Entry<Integer, Long>> iter = _failedBrokers.entrySet().iterator(); iter.hasNext(); ) {
      Map.Entry<Integer, Long> entry = iter.next();
      sb.append(entry.getKey()).append("=").append(entry.getValue());
      if (iter.hasNext()) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  private void parsePersistedFailedBrokers(String failedBrokerListString) {
    if (failedBrokerListString != null && !failedBrokerListString.isEmpty()) {
      String[] entries = failedBrokerListString.split(",");
      for (String entry : entries) {
        String[] idAndTimestamp = entry.split("=");
        if (idAndTimestamp.length != 2) {
          throw new IllegalStateException(
              "The persisted failed broker string cannot be parsed. The string is " + failedBrokerListString);
        }
        _failedBrokers.putIfAbsent(Integer.parseInt(idAndTimestamp[0]), Long.parseLong(idAndTimestamp[1]));
      }
    }
  }

  private void reportBrokerFailures() {
    if (!_failedBrokers.isEmpty()) {
      Map<String, Object> parameterConfigOverrides = new HashMap<>(3);
      parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
      parameterConfigOverrides.put(FAILED_BROKERS_OBJECT_CONFIG, failedBrokers());
      parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_CONFIG, _time.milliseconds());

      BrokerFailures brokerFailures = _kafkaCruiseControl.config().getConfiguredInstance(KafkaCruiseControlConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                                                         BrokerFailures.class,
                                                                                         parameterConfigOverrides);
      _anomalies.add(brokerFailures);
    }
  }

  /**
   * The zookeeper listener for failure detection.
   */
  private class BrokerFailureListener implements IZkChildListener {

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) {
      detectBrokerFailures(currentChildren.stream().map(Integer::parseInt).collect(toSet()));
    }
  }

  /**
   * Serde class for ZkClient.
   */
  private static class ZkStringSerializer implements ZkSerializer {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
      return ((String) data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

}
