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

import java.time.Instant;
import java.util.UUID;

/**
 * Job run status. Include which node it running on, start time, finish time and so on.
 *
 * @author Wei Gao
 */
public class JobRunStatus {
  private final UUID nodeId;
  private Instant startTime;
  // Last run finish time
  private Instant finishTime;
  // Last run status
  private JobInstanceState state;

  public JobRunStatus(UUID nodeId) {
    this.nodeId = nodeId;
    state = JobInstanceState.SCHEDULED;
  }

  public UUID getNodeId() {
    return nodeId;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public void setStartTime(Instant startTime) {
    this.startTime = startTime;
  }

  public Instant getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(Instant finishTime) {
    this.finishTime = finishTime;
  }

  public JobInstanceState getState() {
    return state;
  }

  public void setState(JobInstanceState state) {
    this.state = state;
  }
}
