/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.concurrent.TimeoutException;


/**
 * The interface for getting the broker capacity. Users should implement this interface so Cruise Control can
 * optimize the cluster according to the capacity of each broker.
 *
 */
public interface BrokerCapacityConfigResolver extends CruiseControlConfigurable, AutoCloseable {
  /**
   * Get the capacity of a broker based on rack, host and broker id.
   * The response must contain all the resources defined in {@link Resource}. The units for each resource are:
   * DISK - MegaBytes
   * CPU - Percentage (0 - 100)
   * Network Inbound - KB/s
   * Network Outbounds - KB/s
   *
   * The response also contains the number of CPU cores and may contain disk capacities by logDirs (i.e. for JBOD).
   * May estimate the capacity of a broker, if it is not directly available.
   *
   * @param rack The rack of the broker
   * @param host The host of the broker
   * @param brokerId The id of the broker
   * @param timeoutMs The timeout in millisecond.
   * @param allowCapacityEstimation Whether allow resolver to estimate broker capacity if resolver is unable to get
   *                                capacity information of the broker.
   * @return The capacity of each resource for the broker
   * @throws TimeoutException if resolver is unable to resolve broker capacity in time.
   */
  BrokerCapacityInfo capacityForBroker(String rack, String host, int brokerId, long timeoutMs, boolean allowCapacityEstimation)
      throws TimeoutException;
}
