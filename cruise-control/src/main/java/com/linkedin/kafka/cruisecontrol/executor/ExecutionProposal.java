/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.model.ClusterModel.ReplicaPlacementInfo;

/**
 * The execution proposal corresponding to a particular partition.
 */
public class ExecutionProposal {
  private static final String TOPIC_PARTITION = "topicPartition";
  private static final String OLD_LEADER = "oldLeader";
  private static final String OLD_REPLICAS = "oldReplicas";
  private static final String NEW_REPLICAS = "newReplicas";

  private final TopicPartition _tp;
  private final long _partitionSize;
  private final ReplicaPlacementInfo _oldLeader;
  private final List<ReplicaPlacementInfo> _oldReplicas;
  private final List<ReplicaPlacementInfo> _newReplicas;
  // Replicas to add are the replicas which are originally not hosted by the broker.
  private final Set<ReplicaPlacementInfo> _replicasToAdd;
  // Replicas to remove are the replicas which are no longer hosted by the broker.
  private final Set<ReplicaPlacementInfo> _replicasToRemove;
  // Replicas to move are the replicas are originally hosted by the broker and need to relocated to another disk of the hosting broker.
  private final Map<Integer, ReplicaPlacementInfo> _replicasToMoveByBroker;

  /**
   * Construct an execution proposals.
   * Note that the new leader will be the first replica in the new replica list after the proposal is executed.
   *
   * @param tp the topic partition of this execution proposal
   * @param partitionSize the size of the partition.
   * @param oldLeader the old leader of the partition to determine if leader movement is needed.
   * @param oldReplicas the old replicas for rollback. (Rollback is not supported until KAFKA-6304)
   * @param newReplicas the new replicas of the partition in this order.
   */
  public ExecutionProposal(TopicPartition tp,
                           long partitionSize,
                           ReplicaPlacementInfo oldLeader,
                           List<ReplicaPlacementInfo> oldReplicas,
                           List<ReplicaPlacementInfo> newReplicas) {
    _tp = tp;
    _partitionSize = partitionSize;
    _oldLeader = oldLeader;
    // Allow the old replicas to be empty for partition addition.
    _oldReplicas = oldReplicas == null ? Collections.emptyList() : oldReplicas;
    _newReplicas = newReplicas;
    validate();

    // Populate replicas to add, to remove and to move across disk.
    Set<Integer> newBrokerList = _newReplicas.stream().mapToInt(ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toSet());
    Set<Integer> oldBrokerList = _oldReplicas.stream().mapToInt(ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toSet());
    _replicasToAdd = _newReplicas.stream().filter(r -> !oldBrokerList.contains(r.brokerId())).collect(Collectors.toSet());
    _replicasToRemove = _oldReplicas.stream().filter(r -> !newBrokerList.contains(r.brokerId())).collect(Collectors.toSet());
    _replicasToMoveByBroker = new HashMap<>();
    newReplicas.stream().filter(r -> !_replicasToAdd.contains(r) && !_oldReplicas.contains(r))
               .forEach(r -> _replicasToMoveByBroker.put(r.brokerId(), r));

    // Verify the proposal will not generate both inter-broker movement and intra-broker replica movement at the same time.
    if (!_replicasToAdd.isEmpty() && !_replicasToMoveByBroker.isEmpty()) {
      throw new IllegalArgumentException("Change from " + _oldReplicas + " to " + _newReplicas + " will generate both "
                                         + "intra-broker and inter-broker replica movements.");
    }
  }

  private boolean brokerOrderMatched(Node[] currentOrderedReplicas, List<ReplicaPlacementInfo> replicas) {
    if (replicas.size() != currentOrderedReplicas.length) {
      return false;
    }

    for (int i = 0; i < replicas.size(); i++) {
      if (currentOrderedReplicas[i].id() != replicas.get(i).brokerId()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check whether the successful completion of inter-broker replica movement from this proposal is reflected in the current
   * ordered replicas in the given cluster.
   *
   * @param currentOrderedReplicas Current ordered replica list from the cluster.
   * @return True if successfully completed, false otherwise.
   */
  public boolean isInterBrokerMovementCompleted(Node[] currentOrderedReplicas) {
    return brokerOrderMatched(currentOrderedReplicas, _newReplicas);
  }

  /**
   * Check whether the proposal abortion of inter-broker replica movement from this proposal is reflected in the current
   * ordered replicas in the given cluster.
   * There could be a race condition that when we abort a task, it is already completed.
   * In that case, we treat it as aborted as well.
   *
   * @param currentOrderedReplicas Current ordered replica list from the cluster.
   * @return True if aborted, false otherwise.
   */
  public boolean isInterBrokerMovementAborted(Node[] currentOrderedReplicas) {
    return isInterBrokerMovementCompleted(currentOrderedReplicas) || brokerOrderMatched(currentOrderedReplicas, _oldReplicas);
  }

  /**
   * @return the topic for this proposal.
   */
  public String topic() {
    return _tp.topic();
  }

  /**
   * @return the partition id for this proposal.
   */
  public int partitionId() {
    return _tp.partition();
  }

  /**
   * @return the TopicPartition of this proposal.
   */
  public TopicPartition topicPartition() {
    return _tp;
  }

  /**
   * @return the old leader of the partition before the executing the proposal.
   */
  public ReplicaPlacementInfo oldLeader() {
    return _oldLeader;
  }

  /**
   * @return the new leader of the partition after executing the proposal.
   */
  public ReplicaPlacementInfo newLeader() {
    return _newReplicas.get(0);
  }

  /**
   * @return the replica list of the partition before executing the proposal.
   */
  public List<ReplicaPlacementInfo> oldReplicas() {
    return _oldReplicas;
  }

  /**
   * @return the new replica list fo the partition after executing the proposal.
   */
  public List<ReplicaPlacementInfo> newReplicas() {
    return _newReplicas;
  }

  /**
   * @return the replicas that exist in new replica list but not in old replica list.
   */
  public Set<ReplicaPlacementInfo> replicasToAdd() {
    return _replicasToAdd;
  }

  /**
   * @return the replicas that exist in old replica list but not in the new replica list.
   */
  public Set<ReplicaPlacementInfo> replicasToRemove() {
    return _replicasToRemove;
  }

  /**
   * @return the replicas that exist in both old and new replica list but its hosted disk has changed.
   */
  public Map<Integer, ReplicaPlacementInfo> replicasToMoveByBroker() {
    return _replicasToMoveByBroker;
  }

  /**
   * @return whether the proposal involves a replica action. (replica movement, addition, deletion, reorder)
   */
  public boolean hasReplicaAction() {
    return !_oldReplicas.equals(_newReplicas);
  }

  /**
   * @return whether the proposal involves a leader action. i.e. leader movement.
   */
  public boolean hasLeaderAction() {
    return _oldLeader != _newReplicas.get(0);
  }

  /**
   * @return the total number of bytes to move cross brokers involved in this proposal.
   */
  public long interBrokerDataToMoveInMB() {
    return _replicasToAdd.size() * _partitionSize;
  }

  /**
   * @return the total number of bytes to move cross disks within the broker involved in this proposal.
   */
  public long intraBrokerDataToMoveInMB() {
    return  _partitionSize;
  }

  /**
   * @return the total number of bytes to move involved in this proposal.
   */
  public long dataToMoveInMB() {
    return (_replicasToAdd.size() + _replicasToMoveByBroker.size()) * _partitionSize;
  }

  private void validate() {
    // Verify old leader exists.
    if (_oldLeader.brokerId() >= 0 && !_oldReplicas.contains(_oldLeader)) {
      throw new IllegalArgumentException(String.format("The old leader %s does not exit in the old replica list %s",
                                                       _oldLeader, _oldReplicas));
    }
    // verify empty new replica list.
    if (_newReplicas == null || _newReplicas.isEmpty()) {
      throw new IllegalArgumentException("The new replica list " + _newReplicas + " cannot be empty.");
    }
    // Verify duplicates
    Set<ReplicaPlacementInfo> checkSet = new HashSet<>(_newReplicas);
    if (checkSet.size() != _newReplicas.size()) {
      throw new IllegalArgumentException("The new replicas list " + _newReplicas + " has duplicate replica.");
    }
  }

  /**
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> proposalMap = new HashMap<>(4);
    proposalMap.put(TOPIC_PARTITION, _tp);
    proposalMap.put(OLD_LEADER, _oldLeader);
    proposalMap.put(OLD_REPLICAS, _oldReplicas);
    proposalMap.put(NEW_REPLICAS, _newReplicas);
    return proposalMap;
  }

  @Override
  public String toString() {
    return String.format("{%s, oldLeader: %s, %s -> %s}", _tp, _oldLeader, _oldReplicas, _newReplicas);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ExecutionProposal)) {
      return false;
    }

    if (this == other) {
      return true;
    }

    ExecutionProposal otherProposal = (ExecutionProposal) other;

    return _tp.equals(otherProposal._tp)
        && _oldLeader == otherProposal._oldLeader
        && _oldReplicas.equals(otherProposal._oldReplicas)
        && _newReplicas.equals(otherProposal._newReplicas);
  }

  @Override
  public int hashCode() {
    int result = _tp.hashCode();
    result = 31 * result + _oldLeader.hashCode();
    for (ReplicaPlacementInfo replica : _oldReplicas) {
      result = 31 * result + replica.hashCode();
    }
    for (ReplicaPlacementInfo replica : _newReplicas) {
      result = 31 * replica.hashCode();
    }
    return result;
  }
}
