/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import java.io.UnsupportedEncodingException;
import java.util.List;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#ADD_BROKER} and
 * {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#REMOVE_BROKER}.
 *
 * <pre>
 * 1. Decommission a broker
 *    POST /kafkacruisecontrol/remove_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]
 *    &amp;throttle_removed_broker=[true/false]&amp;goals=[goal1,goal2...]&amp;allow_capacity_estimation=[true/false]
 *    &amp;concurrent_inter_broker_partition_movements_per_broker=[POSITIVE-INTEGER]&amp;concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;kafka_assigner=[true/false]
 *    &amp;use_ready_default_goals=[true/false]&amp;verbose=[true/false]&amp;exclude_recently_demoted_brokers=[true/false]
 *    &amp;exclude_recently_removed_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *
 * 2. Add a broker
 *    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]
 *    &amp;throttle_added_broker=[true/false]&amp;goals=[goal1,goal2...]&amp;allow_capacity_estimation=[true/false]
 *    &amp;concurrent_inter_broker_partition_movements_per_broker=[POSITIVE-INTEGER]&amp;concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;kafka_assigner=[true/false]
 *    &amp;use_ready_default_goals=[true/false]&amp;verbose=[true/false]&amp;exclude_recently_demoted_brokers=[true/false]
 *    &amp;exclude_recently_removed_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 * </pre>
 */
public class AddedOrRemovedBrokerParameters extends GoalBasedOptimizationParameters {
  private List<Integer> _brokerIds;
  private Integer _concurrentInterBrokerPartitionMovements;
  private Integer _concurrentLeaderMovements;
  private boolean _dryRun;
  private boolean _throttleAddedOrRemovedBrokers;
  private boolean _skipHardGoalCheck;
  private ReplicaMovementStrategy _replicaMovementStrategy;
  private final KafkaCruiseControlConfig _config;

  public AddedOrRemovedBrokerParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request);
    _config = config;
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _brokerIds = ParameterUtils.brokerIds(_request);
    _dryRun = ParameterUtils.getDryRun(_request);
    _throttleAddedOrRemovedBrokers = ParameterUtils.throttleAddedOrRemovedBrokers(_request, _endPoint);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true, false);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false, false);
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_request);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_request, _config);
  }

  public List<Integer> brokerIds() {
    return _brokerIds;
  }

  public Integer concurrentInterBrokerPartitionMovements() {
    return _concurrentInterBrokerPartitionMovements;
  }

  public Integer concurrentLeaderMovements() {
    return _concurrentLeaderMovements;
  }

  public boolean dryRun() {
    return _dryRun;
  }

  public boolean throttleAddedOrRemovedBrokers() {
    return _throttleAddedOrRemovedBrokers;
  }

  public boolean skipHardGoalCheck() {
    return _skipHardGoalCheck;
  }

  public ReplicaMovementStrategy replicaMovementStrategy() {
    return _replicaMovementStrategy;
  }
}
