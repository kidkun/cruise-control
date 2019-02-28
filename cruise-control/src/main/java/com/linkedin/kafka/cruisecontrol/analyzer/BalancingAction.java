/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Objects;
import org.apache.kafka.common.TopicPartition;
import java.util.HashMap;
import java.util.Map;


/**
 * Represents the load balancing operation over a replica for Kafka Load GoalOptimizer.
 */
public class BalancingAction {
  private static final String TOPIC_PARTITION = "topicPartition";
  private static final String SOURCE_BROKER_ID = "sourceBrokerId";
  private static final String SOURCE_BROKER_LOGDIR = "sourceBrokerLogdir";
  private static final String DESTINATION_BROKER_ID = "destinationBrokerId";
  private static final String DESTINATION_BROKER_LOGIR = "destinationBrokerLogdir";
  private static final String DESTINATION_TOPIC_PARTITION = "destinationTopicPartition";
  private static final String ACTION_TYPE = "actionType";
  private final TopicPartition _tp;
  private final Integer _sourceBrokerId;
  private final String _sourceBrokerLogdir;
  private final String _destinationBrokerLogdir;
  private final Integer _destinationBrokerId;
  private final ActionType _actionType;
  private final TopicPartition _destinationTp;

  /**
   * Constructor for creating a balancing proposal with given topic partition, source and destination broker id, and
   * balancing action.
   *
   * @param tp                  Topic partition of the replica.
   * @param sourceBrokerId      Source broker id of the replica.
   * @param destinationBrokerId Destination broker id of the replica.
   * @param actionType          Action type.
   */
  public BalancingAction(TopicPartition tp,
                         Integer sourceBrokerId,
                         Integer destinationBrokerId,
                         ActionType actionType) {
    this(tp, sourceBrokerId, destinationBrokerId, actionType, tp);
  }

  /**
   * Constructor for creating a balancing proposal with given topic partitions, source and destination broker id,
   * and the topic partition of replica to swap with.
   *
   * @param sourceTp            Topic partition of the source replica.
   * @param sourceBrokerId      Source broker id of the replica.
   * @param destinationBrokerId Destination broker id of the replica.
   * @param actionType          Action type.
   * @param destinationTp       Topic partition of the replica to swap with.
   */
  public BalancingAction(TopicPartition sourceTp,
                         Integer sourceBrokerId,
                         Integer destinationBrokerId,
                         ActionType actionType,
                         TopicPartition destinationTp) {
    this(sourceTp, sourceBrokerId, null, destinationBrokerId, null, actionType, destinationTp);
  }

  /**
   * Constructor for creating a balancing proposal with given topic partitions, source and destination broker id and logdir,
   * and the topic partition of replica to swap with.
   *
   * @param sourceTp                Topic partition of the source replica.
   * @param sourceBrokerId          Source broker id of the replica.
   * @param sourceBrokerLogdir      Source broker logdir of the replica.
   * @param destinationBrokerId     Destination broker id of the replica.
   * @param destinationBrokerLogdir Destination broker logdir of the replica.
   * @param actionType              Action type.
   * @param destinationTp           Topic partition of the replica to swap with.
   */
  public BalancingAction(TopicPartition sourceTp,
                         Integer sourceBrokerId,
                         String sourceBrokerLogdir,
                         Integer destinationBrokerId,
                         String destinationBrokerLogdir,
                         ActionType actionType,
                         TopicPartition destinationTp) {
    _tp = sourceTp;
    _sourceBrokerId = sourceBrokerId;
    _sourceBrokerLogdir = sourceBrokerLogdir;
    _destinationBrokerId = destinationBrokerId;
    _destinationBrokerLogdir = destinationBrokerLogdir;
    _actionType = actionType;
    _destinationTp = destinationTp;
    validate();
  }

  private void validate() {
    switch (_actionType) {
      case REPLICA_ADDITION:
        if (_destinationBrokerId == null) {
          throw new IllegalArgumentException("The destination broker cannot be null for balancing action " + this);
        } else if (_sourceBrokerId != null) {
          throw new IllegalArgumentException("The source broker should be null for balancing action " + this);
        }
        break;
      case REPLICA_DELETION:
        if (_destinationBrokerId != null) {
          throw new IllegalArgumentException("The destination broker should be null for balancing action " + this);
        } else if (_sourceBrokerId == null) {
          throw new IllegalArgumentException("The source broker cannot be null for balancing action " + this);
        }
        break;
      case REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
      case REPLICA_SWAP:
        if (_destinationBrokerId == null) {
          throw new IllegalArgumentException("The destination broker cannot be null for balancing action " + this);
        } else if (_sourceBrokerId == null) {
          throw new IllegalArgumentException("The source broker cannot be null for balancing action " + this);
        }
        if (_sourceBrokerLogdir != null || _destinationBrokerLogdir != null) {
          if (_sourceBrokerLogdir == null) {
            throw new IllegalArgumentException("The source disk is null while destination disk is not null "
                                               + "for balancing action " + this);
          }
          if (_destinationBrokerLogdir == null) {
            throw new IllegalArgumentException("The destination disk is null while source disk is not null."
                                               + "for balancing action " + this);
          }
          if (!_sourceBrokerId.equals(_destinationBrokerId)) {
            throw new IllegalArgumentException("Replica movement between disks across broker is not supported "
                                               + "for balancing action " + this);
          }
        }
        break;
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  /**
   * Get the destination topic partition to swap with.
   */
  public TopicPartition destinationTopicPartition() {
    return _destinationTp;
  }

  /**
   * Get topic name of the replica to swap with at the destination.
   */
  public String destinationTopic() {
    return _destinationTp.topic();
  }

  /**
   * Get the partition Id that is impacted by the balancing action.
   */
  public int partitionId() {
    return _tp.partition();
  }

  /**
   * Get topic name of the impacted partition.
   */
  public String topic() {
    return _tp.topic();
  }

  /**
   * Get topic partition.
   */
  public TopicPartition topicPartition() {
    return _tp;
  }

  /**
   * Get the source broker logdir.
   */
  public String sourceBrokerLogdir() {
    return _sourceBrokerLogdir;
  }

  /**
   * Get the destination broker logdir.
   */
  public String destinationBrokerLogdir() {
    return _destinationBrokerLogdir;
  }

  /**
   * Get the source broker id that is impacted by the balancing action.
   */
  public Integer sourceBrokerId() {
    return _sourceBrokerId;
  }

  /**
   * Get the destination broker Id.
   */
  public Integer destinationBrokerId() {
    return _destinationBrokerId;
  }

  /**
   * Get the type of action that provides balancing.
   */
  public ActionType balancingAction() {
    return _actionType;
  }

  /**
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> proposalMap = new HashMap<>();
    proposalMap.put(TOPIC_PARTITION, _tp);
    proposalMap.put(SOURCE_BROKER_ID, _sourceBrokerId);
    proposalMap.put(DESTINATION_BROKER_ID, _destinationBrokerId);
    proposalMap.put(DESTINATION_TOPIC_PARTITION, _destinationTp);
    proposalMap.put(ACTION_TYPE, _actionType);
    // _sourceBrokerLogdir and _destinationBrokerLogdir are guaranteed to both be null or neither be null.
    if (_sourceBrokerLogdir != null) {
      proposalMap.put(SOURCE_BROKER_LOGDIR, _sourceBrokerLogdir);
      proposalMap.put(DESTINATION_BROKER_LOGIR, _destinationBrokerLogdir);
    }
    return proposalMap;
  }

  /**
   * Get string representation of this balancing proposal.
   */
  @Override
  public String toString() {
    String actSymbol = _actionType.equals(ActionType.REPLICA_SWAP) ? "<->" : "->";
    return String.format("(%s%s%s, %d%s%s%d%s, %s)",
                         _tp, actSymbol, _destinationTp,
                         _sourceBrokerId, _sourceBrokerLogdir == null ? "" : _sourceBrokerLogdir, actSymbol,
                         _destinationBrokerId, _destinationBrokerLogdir == null ? "" : _destinationBrokerLogdir, _actionType);
  }

  /**
   * Compare the given object with this object.
   *
   * @param other Other object to be compared with this object.
   * @return True if other object equals this object, false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BalancingAction)) {
      return false;
    }

    if (this == other) {
      return true;
    }

    BalancingAction otherAction = (BalancingAction) other;
    return Objects.equals(_sourceBrokerId, otherAction._sourceBrokerId)
           && Objects.equals(_sourceBrokerLogdir, otherAction._sourceBrokerLogdir)
           && Objects.equals(_tp, otherAction._tp)
           && Objects.equals(_destinationBrokerId, otherAction._destinationBrokerId)
           && Objects.equals(_destinationBrokerLogdir, otherAction._destinationBrokerLogdir)
           && Objects.equals(_destinationTp, otherAction._destinationTp)
           && Objects.equals(_actionType, otherAction._actionType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_tp, _sourceBrokerId, _sourceBrokerLogdir, _destinationBrokerId, _destinationBrokerLogdir,
                        _actionType, _destinationTp);
  }
}
