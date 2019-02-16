/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A class that holds the disk information of a broker, including its liveness, capacity and load.
 * A disk object is created as part of a broker structure.
 */
public class Disk  implements Comparable<Disk> {
  private static final double DEAD_DISK_CAPACITY = -1.0;

  public enum State {
    ALIVE, DEAD
  }

  private final String _logDir;
  private double _diskCapacity;
  private final Set<Replica> _replicas;
  private State _state;
  private Broker _broker;
  private double _load;
  private final Map<String, SortedReplicas> _sortedReplicas;

  /**
   * Constructor for Disk class.
   *
   * @param logDir         The log directory maps to disk.
   * @param broker         The broker of the disk.
   * @param diskCapacity   The capacity of the disk.
   */
  Disk(String logDir, Broker broker, double diskCapacity) {
    _logDir = logDir;
    _broker = broker;
    _replicas = new HashSet<>();
    _load = 0;
    _sortedReplicas = new HashMap<>();

    if (diskCapacity < 0) {
      _diskCapacity = DEAD_DISK_CAPACITY;
      _state = State.DEAD;
    } else {
      _diskCapacity = diskCapacity;
      _state = State.ALIVE;
    }
  }

  public String logDir() {
    return _logDir;
  }

  public double capacity() {
    return _diskCapacity;
  }

  public Disk.State state() {
    return _state;
  }

  public Set<Replica> replicas() {
    return Collections.unmodifiableSet(_replicas);
  }

  public Broker broker() {
    return _broker;
  }

  public double load() {
    return _load;
  }

  /**
   * Set Disk status.
   *
   * @param newState The new state of the broker.
   */
  public void setState(Disk.State newState) {
    _state = newState;
    if (_state == State.DEAD) {
      _diskCapacity = DEAD_DISK_CAPACITY;
    }
  }

  /**
   * Add replica to the disk.
   *
   * @param replica Replica to be added to the current disk.
   */
  void addReplica(Replica replica, boolean isOriginal) {
    if (_replicas.contains(replica)) {
      throw new IllegalStateException(String.format("Disk %s already has replica %s", _logDir, replica.topicPartition()));
    }
    _load += replica.load().expectedUtilizationFor(Resource.DISK);
    _replicas.add(replica);
    if (isOriginal) {
      replica.setOriginalDisk(this);
    }
    replica.setDisk(this);
    _sortedReplicas.values().forEach(sr -> sr.add(replica));
  }

  /**
   * Remove replica from the disk.
   *
   * @param replica Replica to be removed from the current disk.
   */
  void removeReplica(Replica replica) {
    if (!_replicas.contains(replica)) {
      throw new IllegalStateException(String.format("Disk %s does not has replica %s", _logDir, replica.topicPartition()));
    }
    _load -= replica.load().expectedUtilizationFor(Resource.DISK);
    _replicas.remove(replica);
    _sortedReplicas.values().forEach(sr -> sr.remove(replica));
  }

  /**
   * Track the sorted replicas using the given score function. The sort first uses the priority function to
   * sort the replicas, then use the score function to sort the replicas. The priority function is useful
   * to priorities a particular type of replicas, e.g leader replicas, immigrant replicas, etc.
   *
   * @param sortName the name of the tracked sorted replicas.
   * @param selectionFunc the selection function to decide which replicas to include.
   * @param priorityFunc the priority function to sort replicas.
   * @param scoreFunc the score function to sort replicas.
   */
  public void trackSortedReplicas(String sortName,
      Function<Replica, Boolean> selectionFunc,
      Function<Replica, Integer> priorityFunc,
      Function<Replica, Double> scoreFunc) {
    _sortedReplicas.putIfAbsent(sortName, new SortedReplicas(_broker, this, selectionFunc, priorityFunc, scoreFunc, true));
  }

  public void trackSortedReplicas(String sortName,
      Function<Replica, Boolean> selectionFunc,
      Function<Replica, Double> scoreFunc) {
    _sortedReplicas.putIfAbsent(sortName, new SortedReplicas(_broker, this, selectionFunc, (r1) -> 0, scoreFunc, true));
  }

  /**
   * Untrack the sorted replicas for the given sort name. This helps release memory.
   *
   * @param sortName the name of the tracked sorted replicas.
   */
  public void untrackSortedReplicas(String sortName) {
    _sortedReplicas.remove(sortName);
  }

  /**
   * Get the tracked sorted replicas using the given sort name.
   *
   * @param sortName the sort name.
   * @return the {@link SortedReplicas} for the given sort name.
   */
  public SortedReplicas trackedSortedReplicas(String sortName) {
    SortedReplicas sortedReplicas = _sortedReplicas.get(sortName);
    if (sortedReplicas == null) {
      throw new IllegalStateException("The sort name " + sortName + "  is not found. Make sure trackSortedReplicas() " +
          "has been called for the sort name");
    }
    return sortedReplicas;
  }

  @Override
  public int compareTo(Disk d) {
    return _logDir.compareTo(d.logDir());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof  Disk)) {
      return false;
    }
    return compareTo((Disk) o) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_logDir);
  }

  @Override
  public String toString() {
    return String.format("Disk[logdir=%s,state=%s,capacity=%f,replicaCount=%d]", _logDir, _state, _diskCapacity, _replicas.size());
  }
}
