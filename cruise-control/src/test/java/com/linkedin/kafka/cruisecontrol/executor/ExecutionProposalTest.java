/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.model.ClusterModel.ReplicaPlacementInfo;

/**
 * Unit test for execution proposals.
 */
public class ExecutionProposalTest {
  private final TopicPartition TP = new TopicPartition("topic", 0);
  private final ReplicaPlacementInfo r0 =  new ReplicaPlacementInfo(0);
  private final ReplicaPlacementInfo r1 =  new ReplicaPlacementInfo(1);
  private final ReplicaPlacementInfo r2 =  new ReplicaPlacementInfo(2);

  private final ReplicaPlacementInfo r0d0 =  new ReplicaPlacementInfo(0, "tmp0");
  private final ReplicaPlacementInfo r0d1 =  new ReplicaPlacementInfo(0, "tmp1");
  private final ReplicaPlacementInfo r1d1 =  new ReplicaPlacementInfo(1, "tmp1");

  @Test (expected = IllegalArgumentException.class)
  public void testNullNewReplicaList() {
    new ExecutionProposal(TP, 0, r1, Arrays.asList(r0, r1), null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testEmptyNewReplicaList() {
    new ExecutionProposal(TP, 0, r1, Arrays.asList(r0, r1), Collections.emptyList());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDuplicateReplicaInNewReplicaList() {
    new ExecutionProposal(TP, 0, r1, Arrays.asList(r0, r1), Arrays.asList(r2, r2));
  }

  @Test (expected = IllegalArgumentException.class)
  public void testOldLeaderMissingFromOldReplicas() {
    new ExecutionProposal(TP, 0, r2, Arrays.asList(r0, r1), Arrays.asList(r1, r2));
  }

  @Test
  public void testIntraBrokerReplicaMovements() {
    ExecutionProposal p = new ExecutionProposal(TP, 0, r0d0, Arrays.asList(r0d0, r1d1), Arrays.asList(r0d1, r1d1));
    Assert.assertEquals(1, p.replicasToMoveByBroker().size());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testMingleReplicaMovements() {
    new ExecutionProposal(TP, 0, r0d0, Arrays.asList(r0d0, r1), Arrays.asList(r0d1, r2));
  }
}
