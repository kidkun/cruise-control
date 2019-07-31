/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * Parameters for {@link CruiseControlEndPoint#TOPIC_CONFIGURATION}.
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *   <li>If topics to change replication factor and target replication factor is not specified in URL, they can also be
 *   specified in request body. The body format will be
 *   {
 *     target_replicaiton_factor_1 : topic_regex_1
 *     target_replication_factor_2 : topic_regex_2
 *     ...
 *   }.
 *   If it is specified both in URL and body, what specified in URL will be picked up by Cruise Control.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/topic_configuration?json=[true/false]&amp;verbose=[true/false]&amp;topic=[topic]
 *    &amp;replication_factor=[target_replication_factor]&amp;skip_rack_awareness_check=[true/false]
 *    &amp;dryRun=[true/false]&amp;goals=[goal1,goal2...]&amp;skip_hard_goal_check=[true/false]
 *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;exclude_recently_demoted_brokers=[true/false]
 *    &amp;exclude_recently_removed_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;review_id=[id]&amp;replication_throttle=[bytes_per_second]
 * </pre>
 */
public class TopicConfigurationParameters extends GoalBasedOptimizationParameters {
  protected Map<Short, Set<Pattern>> _topicPatternsByReplicationFactor;
  protected boolean _skipRackAwarenessCheck;
  protected Integer _reviewId;
  protected boolean _dryRun;
  protected Integer _concurrentInterBrokerPartitionMovements;
  protected Integer _concurrentLeaderMovements;
  protected boolean _skipHardGoalCheck;
  protected ReplicaMovementStrategy _replicaMovementStrategy;
  protected Long _replicationThrottle;

  public TopicConfigurationParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    Pattern topic = ParameterUtils.topic(_request);
    Short replicationFactor = ParameterUtils.replicationFactor(_request);
    if (topic != null && replicationFactor != null) {
      _topicPatternsByReplicationFactor = Collections.singletonMap(replicationFactor, Collections.singleton(topic));
    } else if (topic == null && replicationFactor == null) {
      _topicPatternsByReplicationFactor = ParameterUtils.topicPatternsByReplicationFactor(_request);
    } else if (topic == null) {
      throw new UserRequestException("Topic to update configuration is not specified in URL while target replication factor is specified.");
    } else {
      throw new UserRequestException("Topic's replication factor is not specified in URL while subject topic is specified.");
    }
    if (_topicPatternsByReplicationFactor.keySet().stream().anyMatch(rf -> rf < 1)) {
      throw new IllegalArgumentException("Target replication factor cannot be set to smaller than 1.");
    }
    boolean twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_request, twoStepVerificationEnabled);
    _skipRackAwarenessCheck = ParameterUtils.skipRackAwarenessCheck(_request);
    _dryRun = ParameterUtils.getDryRun(_request);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true, false);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false, false);
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_request);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_request, _config);
    _replicationThrottle = ParameterUtils.replicationThrottle(_request, _config);
  }

  public Map<Short, Set<Pattern>> topicPatternsByReplicationFactor() {
    return _topicPatternsByReplicationFactor;
  }

  public boolean skipRackAwarenessCheck() {
    return _skipRackAwarenessCheck;
  }

  public boolean dryRun() {
    return _dryRun;
  }

  public Integer concurrentInterBrokerPartitionMovements() {
    return _concurrentInterBrokerPartitionMovements;
  }

  public Integer concurrentLeaderMovements() {
    return _concurrentLeaderMovements;
  }

  public boolean skipHardGoalCheck() {
    return _skipHardGoalCheck;
  }

  public ReplicaMovementStrategy replicaMovementStrategy() {
    return _replicaMovementStrategy;
  }

  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }

  public Long replicationThrottle() {
    return _replicationThrottle;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}