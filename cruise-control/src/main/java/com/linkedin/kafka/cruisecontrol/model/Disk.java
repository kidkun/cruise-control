/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A class that holds the disk information of a broker, including its liveness, capacity and load.
 * A disk object is created as part of a broker structure.
 */
public class Disk {
  private static final double DEAD_DISK_CAPACITY = -1.0;

  public enum State {
    ALIVE, DEAD
  }

  private final String _logDir;
  private double _diskCapacity;
  private final Set<Replica> _replicas;
  private State _state;

  /**
   * Constructor for Disk class.
   *
   * @param logDir         The log directory in Kafka which resides on this disk.
   * @param diskCapacity   The capacity of the disk.
   */
  Disk(String logDir, double diskCapacity) {
    _logDir = logDir;
    _replicas = new HashSet<>();

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
  void addReplicas(Replica replica) {
    if (_replicas.contains(replica)) {
      throw new IllegalStateException(String.format("disk %s already has replica %s", _logDir, replica.topicPartition()));
    }
    _replicas.add(replica);
  }

  @Override
  public String toString() {
    return String.format("Disk[logdir=%s,state=%s,capacity=%f,replicaCount=%d]", _logDir, _state, _diskCapacity, _replicas.size());
  }
}
