/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * The async runnable for {@link KafkaCruiseControl#rebalance(List, boolean, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, Integer, Integer, boolean, Pattern,
 * Long, ReplicaMovementStrategy, String, boolean, boolean, boolean, boolean, Set)}
 */
class RebalanceRunnable extends OperationRunnable {
  private final List<String> _goals;
  private final boolean _dryRun;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final Integer _concurrentInterBrokerPartitionMovements;
  private final Integer _concurrentLeaderMovements;
  private final boolean _skipHardGoalCheck;
  private final Pattern _excludedTopics;
  private final String _uuid;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final ReplicaMovementStrategy _replicaMovementStrategy;
  private final boolean _ignoreProposalCache;
  private final Set<Integer> _destinationBrokerIds;
  private final KafkaCruiseControlConfig _config;

  RebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                    OperationFuture future,
                    RebalanceParameters parameters,
                    String uuid,
                    KafkaCruiseControlConfig config) {
    super(kafkaCruiseControl, future);
    _goals = parameters.goals();
    _dryRun = parameters.dryRun();
    _modelCompletenessRequirements = parameters.modelCompletenessRequirements();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _skipHardGoalCheck = parameters.skipHardGoalCheck();
    _excludedTopics = parameters.excludedTopics();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _uuid = uuid;
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _excludeRecentlyRemovedBrokers = parameters.excludeRecentlyRemovedBrokers();
    _ignoreProposalCache = parameters.ignoreProposalCache();
    _destinationBrokerIds = parameters.destinationBrokerIds();
    _config = config;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(_kafkaCruiseControl.rebalance(_goals,
                                                                _dryRun,
                                                                _modelCompletenessRequirements,
                                                                _future.operationProgress(),
                                                                _allowCapacityEstimation,
                                                                _concurrentInterBrokerPartitionMovements,
                                                                _concurrentLeaderMovements,
                                                                _skipHardGoalCheck,
                                                                _excludedTopics,
                                                                null,
                                                                _replicaMovementStrategy,
                                                                _uuid,
                                                                _excludeRecentlyDemotedBrokers,
                                                                _excludeRecentlyRemovedBrokers,
                                                                _ignoreProposalCache,
                                                                false,
                                                                _destinationBrokerIds),
                                  _config);
  }
}
