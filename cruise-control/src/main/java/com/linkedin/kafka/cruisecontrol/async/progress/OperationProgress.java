/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpSession;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.UNIT_INTERVAL_TO_PERCENTAGE;
import static javax.servlet.http.HttpServletResponse.*;


/**
 * A class to track the progress of a task. This class is used to allow different users to trigger
 * an endpoint which may take a while for Cruise Control to respond, e.g. getting a complicated proposal.
 * Cruise Control will use {@link HttpSession} to keep track the progress of such requests and
 * report the progress to the users.
 */
@JsonResponseClass
public class OperationProgress {
  @JsonResponseField(responseStatus = SC_ACCEPTED)
  protected static final String OPERATION = "operation";
  @JsonResponseField(responseStatus = SC_ACCEPTED)
  protected static final String OPERATION_PROGRESS = "operationProgress";
  private boolean _mutable = true;
  private List<OperationStep> _steps = new ArrayList<>();
  private List<Long> _startTimes = new ArrayList<>();
  private String _operation;

  public  OperationProgress() {
    this("");
  }

  public OperationProgress(String operation) {
    _operation = operation;
  }

  /**
   * Add a {@link OperationStep} to the progress.
   * @param step the operation step to add.
   */
  public synchronized void addStep(OperationStep step) {
    ensureMutable();
    _steps.add(step);
    _startTimes.add(System.currentTimeMillis());
  }

  /**
   * Refer this operation progress to another one. This is useful when multiple operations are waiting for the
   * same background task to finish.
   *
   * Once this OperationProgress is referring to another OperationProgress, this OperationProgress becomes immutable
   * to avoid accidental change of the referred OperationProgress.
   *
   * @param other the other operation progress to refer to.
   */
  public void refer(OperationProgress other) {
    // ensure the integrity and avoid dead lock.
    List<OperationStep> steps;
    List<Long> startTimes;
    synchronized (other) {
      steps = other._steps;
      startTimes = other._startTimes;
    }
    synchronized (this) {
      ensureMutable();
      this._steps = steps;
      this._startTimes = startTimes;
      this._mutable = false;
    }
  }

  /**
   * @return The list of operation steps in this operation progress.
   */
  public synchronized List<OperationStep> progress() {
    return Collections.unmodifiableList(_steps);
  }

  /**
   * Clear the progress.
   */
  public synchronized void clear() {
    this._mutable = true;
    _steps.clear();
    _startTimes.clear();
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < _steps.size(); i++) {
      OperationStep step = _steps.get(i);
      long time = (i == _steps.size() - 1 ? System.currentTimeMillis() : _startTimes.get(i + 1)) - _startTimes.get(i);
      sb.append(new StepProgress(step, time).toString());
    }
    return sb.toString();
  }

  /**
   * @return The map describing the progress of the operation.
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> operationProgress = new HashMap<>(2);
    operationProgress.put(OPERATION, _operation);
    operationProgress.put(OPERATION_PROGRESS, getProgress());
    return operationProgress;
  }

  private synchronized Object[] getProgress() {
    Object[] progressArray = new Object[_steps.size()];
    for (int i = 0; i < _steps.size(); i++) {
      OperationStep step = _steps.get(i);
      long time = (i == _steps.size() - 1 ? System.currentTimeMillis() : _startTimes.get(i + 1)) - _startTimes.get(i);
      progressArray[i] = new StepProgress(step, time).getJsonStructure();
    }
    return progressArray;
  }

  private void ensureMutable() {
    if (!_mutable) {
      throw new IllegalStateException("Cannot change this operation progress because it is immutable.");
    }
  }

  @JsonResponseClass
  private static class StepProgress {
    @JsonResponseField(responseStatus = SC_ACCEPTED)
    static final String STEP = "step";
    @JsonResponseField(responseStatus = SC_ACCEPTED)
    static final String DESCRIPTION = "description";
    @JsonResponseField(responseStatus = SC_ACCEPTED)
    static final String TIME_IN_MS = "timeInMs";
    @JsonResponseField(responseStatus = SC_ACCEPTED)
    static final String COMPLETION_PERCENTAGE = "completionPercentage";
    private OperationStep _step;
    private long _duration;

    StepProgress(OperationStep step, long duration) {
      _step = step;
      _duration = duration;
    }

    Map<String, Object> getJsonStructure() {
      Map<String, Object> stepProgressMap = new HashMap<>(4);
      stepProgressMap.put(STEP, _step.name());
      stepProgressMap.put(DESCRIPTION, _step.description());
      stepProgressMap.put(TIME_IN_MS, _duration);
      stepProgressMap.put(COMPLETION_PERCENTAGE, _step.completionPercentage() * UNIT_INTERVAL_TO_PERCENTAGE);
      return stepProgressMap;
    }

    @Override
    public String toString() {
      return String.format("(%6d ms) - (%3.1f%%) %s: %s%n", _duration,
                           _step.completionPercentage() * UNIT_INTERVAL_TO_PERCENTAGE, _step.name(), _step.description());
    }
  }
}
