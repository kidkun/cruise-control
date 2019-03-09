/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.linkedin.kafka.cruisecontrol.model.Broker;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class SingleBrokerStats {
  private static final String HOST = "Host";
  private static final String BROKER = "Broker";
  private static final String BROKER_STATE = "BrokerState";
  private static final String DISK_STATE = "DiskState";
  private static final String DISK_MB = "DiskMB";
  private static final String DISK_PCT = "DiskPct";
  private final String _host;
  private final int _id;
  private final Broker.State _state;
  private final BasicStats _basicStats;
  private final boolean _isEstimated;
  private final Map<String, Double> _utilByDisk;
  private final Map<String, Double> _capacityByDisk;

  SingleBrokerStats(String host, int id, Broker.State state, double diskUtil, double cpuUtil, double leaderBytesInRate,
                    double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate, int numReplicas,
                    int numLeaders, boolean isEstimated, double capacity, Map<String, Double> utilByDisk,
                    Map<String, Double> capacityByDisk) {
    _host = host;
    _id = id;
    _state = state;
    _basicStats = new BasicStats(diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                                 potentialBytesOutRate, numReplicas, numLeaders, capacity);
    _isEstimated = isEstimated;
    _utilByDisk = utilByDisk;
    _capacityByDisk = capacityByDisk;
  }

  public String host() {
    return _host;
  }

  public Broker.State state() {
    return _state;
  }

  public int id() {
    return _id;
  }

  BasicStats basicStats() {
    return _basicStats;
  }

  Map<String, Double> utilByDisk() {
    return Collections.unmodifiableMap(_utilByDisk);
  }

  double pctForDisk(String logdir) {
    return _capacityByDisk.get(logdir) < 0 ? -1 :
                                             _utilByDisk.get(logdir) * 100 / _capacityByDisk.get(logdir);
  }

  public boolean isEstimated() {
    return _isEstimated;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJSONStructure() {
    Map<String, Object> entry = _basicStats.getJSONStructure();
    entry.put(HOST, _host);
    entry.put(BROKER, _id);
    entry.put(BROKER_STATE, _state);
    if (_utilByDisk != null && !_utilByDisk.isEmpty()) {
      Map<String, Object>  diskStates = new HashMap<>(_utilByDisk.size());
      for (String logdir : _utilByDisk.keySet()) {
        Map<String, Object> diskEntry = new HashMap<>(2);
        diskEntry.put(DISK_PCT, pctForDisk(logdir));
        diskEntry.put(DISK_MB, _capacityByDisk.get(logdir));
        diskStates.put(logdir, diskEntry);
      }
      entry.put(DISK_STATE, diskStates);
    }
    return entry;
  }
}