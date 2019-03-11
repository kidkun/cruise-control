/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.linkedin.kafka.cruisecontrol.model.Broker;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.model.Disk.DiskStat;


public class SingleBrokerStats {
  private static final String HOST = "Host";
  private static final String BROKER = "Broker";
  private static final String BROKER_STATE = "BrokerState";
  private static final String DISK_STATE = "DiskState";
  private final String _host;
  private final int _id;
  private final Broker.State _state;
  private final BasicStats _basicStats;
  private final boolean _isEstimated;
  private final Map<String, DiskStat> _diskStatByLogdir;

  SingleBrokerStats(String host, int id, Broker.State state, double diskUtil, double cpuUtil, double leaderBytesInRate,
                    double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate, int numReplicas,
                    int numLeaders, boolean isEstimated, double capacity, Map<String, DiskStat> diskStats) {
    _host = host;
    _id = id;
    _state = state;
    _basicStats = new BasicStats(diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                                 potentialBytesOutRate, numReplicas, numLeaders, capacity);
    _isEstimated = isEstimated;
    _diskStatByLogdir = diskStats;
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

  public Map<String, DiskStat> diskStats() {
    return _diskStatByLogdir;
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
    if (_diskStatByLogdir != null && !_diskStatByLogdir.isEmpty()) {
      Map<String, Object>  diskStates = new HashMap<>(_diskStatByLogdir.size());
      _diskStatByLogdir.forEach((k, v) -> diskStates.put(k, v.getJSONStructure()));
      entry.put(DISK_STATE, diskStates);
    }
    return entry;
  }
}