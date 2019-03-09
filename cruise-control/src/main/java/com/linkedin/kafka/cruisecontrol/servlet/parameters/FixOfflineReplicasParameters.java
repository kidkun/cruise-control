/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#FIX_OFFLINE_REPLICAS}
 *
 * <pre>
 * Fix offline replicas
 *    POST /kafkacruisecontrol/fix_offline_replicas?dryrun=[true/false]&amp;goals=[goal1,goal2...]
 *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_inter_broker_partition_movements_per_broker=[true/false]
 *    &amp;concurrent_leader_movements=[true/false]&amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]
 *    &amp;excluded_topics=[pattern]&amp;use_ready_default_goals=[true/false]&amp;data_from=[valid_windows/valid_partitions]
 *    &amp;replica_movement_strategies=[strategy1,strategy2...]
 * </pre>
 */
public class FixOfflineReplicasParameters extends GoalBasedOptimizationParameters {
  private boolean _dryRun;
  private Integer _concurrentInterBrokerPartitionMovements;
  private Integer _concurrentLeaderMovements;
  private boolean _skipHardGoalCheck;
  private ReplicaMovementStrategy _replicaMovementStrategy;
  private final KafkaCruiseControlConfig _config;

  public FixOfflineReplicasParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request);
    _config = config;
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _dryRun = ParameterUtils.getDryRun(_request);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true, false);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false, false);
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_request);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_request, _config);
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
}
