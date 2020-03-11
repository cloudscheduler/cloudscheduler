/*
 * Copyright (c) 2018. cloudscheduler
 * All rights reserved.
 *
 * Permission is hereby granted, free  of charge, to any person obtaining
 * a  copy  of this  software  and  associated  documentation files  (the
 * "Software"), to  deal in  the Software without  restriction, including
 * without limitation  the rights to  use, copy, modify,  merge, publish,
 * distribute,  sublicense, and/or sell  copies of  the Software,  and to
 * permit persons to whom the Software  is furnished to do so, subject to
 * the following conditions:
 *
 * The  above  copyright  notice  and  this permission  notice  shall  be
 * included in all copies or substantial portions of the Software.
 *
 * THE  SOFTWARE IS  PROVIDED  "AS  IS", WITHOUT  WARRANTY  OF ANY  KIND,
 * EXPRESS OR  IMPLIED, INCLUDING  BUT NOT LIMITED  TO THE  WARRANTIES OF
 * MERCHANTABILITY,    FITNESS    FOR    A   PARTICULAR    PURPOSE    AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE,  ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.github.cloudscheduler.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * JobDefinition status.
 *
 * @author Wei Gao
 */
public class JobDefinitionStatus implements Serializable {
  // JobDefinition id
  private final UUID id;
  // Run count
  private int runCount;
  // Last run schedule time
  private Instant lastScheduleTime;
  private Instant lastCompleteTime;
  private JobDefinitionState state;
  private Map<UUID, JobInstanceState> jobInstanceState;

  /**
   * Constructor.
   *
   * @param id job definition id
   */
  public JobDefinitionStatus(UUID id) {
    if (id == null) {
      throw new NullPointerException("ID is mandatory");
    }
    this.id = id;
    state = JobDefinitionState.CREATED;
    jobInstanceState = new HashMap<>();
  }

  public UUID getId() {
    return id;
  }

  public int getRunCount() {
    return runCount;
  }

  public void setRunCount(int runCount) {
    this.runCount = runCount;
  }

  public Instant getLastScheduleTime() {
    return lastScheduleTime;
  }

  public void setLastScheduleTime(Instant lastScheduleTime) {
    this.lastScheduleTime = lastScheduleTime;
  }

  public Instant getLastCompleteTime() {
    return lastCompleteTime;
  }

  public void setLastCompleteTime(Instant lastCompleteTime) {
    this.lastCompleteTime = lastCompleteTime;
  }

  public JobDefinitionState getState() {
    return state;
  }

  public void setState(JobDefinitionState state) {
    this.state = state;
  }

  public Map<UUID, JobInstanceState> getJobInstanceState() {
    return jobInstanceState;
  }

  public void setJobInstanceState(Map<UUID, JobInstanceState> jobInstanceState) {
    this.jobInstanceState = jobInstanceState;
  }

  @Override
  public int hashCode() {
    return 7 + id.hashCode() * 23;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof JobDefinitionStatus)) {
      return false;
    }
    final JobDefinitionStatus obj = (JobDefinitionStatus) other;
    return id.equals(obj.getId());
  }
}
