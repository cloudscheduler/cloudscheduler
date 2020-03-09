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

package io.github.cloudscheduler.worker;

import io.github.cloudscheduler.JobExecutionContext;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * JobExecutionContext implementation.
 *
 * @author Wei Gao
 */
class JobExecutionContextImpl implements JobExecutionContext {
  private final Node node;
  private final JobDefinition jobDef;
  private final JobDefinitionStatus jobDefStatus;
  private final JobInstance jobIn;

  /**
   * Constructor.
   *
   * @param node node
   * @param jobDef job definition
   * @param jobDefStatus job definition status
   * @param jobIn job instance
   */
  JobExecutionContextImpl(Node node,
                                 JobDefinition jobDef,
                                 JobDefinitionStatus jobDefStatus,
                                 JobInstance jobIn) {
    this.node = node;
    this.jobDef = jobDef;
    this.jobDefStatus = jobDefStatus;
    this.jobIn = jobIn;
  }

  @Override
  public Instant getScheduledTime() {
    return jobIn.getScheduledTime();
  }

  @Override
  public String getJobName() {
    return jobDef.getName();
  }

  @Override
  public int getJobExecuteCount() {
    return jobDefStatus.getRunCount();
  }

  @Override
  public UUID getExecuteNodeId() {
    return node.getId();
  }

  @Override
  public Map<String, Object> getExecutionData() {
    return jobDef.getData();
  }

  @Override
  public Object getExecutionData(String key) {
    return jobDef.getData() == null ? null : jobDef.getData().get(key);
  }

  @Override
  public Instant getLastCompleteTime() {
    return jobDefStatus.getLastCompleteTime();
  }
}
