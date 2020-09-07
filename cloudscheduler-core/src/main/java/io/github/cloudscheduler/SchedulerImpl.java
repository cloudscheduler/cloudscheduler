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

package io.github.cloudscheduler;

import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.service.JobService;
import io.github.cloudscheduler.service.JobServiceImpl;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.zookeeper.ZooKeeper;

/**
 * Scheduler implementation.
 *
 * @author Wei Gao
 */
public class SchedulerImpl implements Scheduler {
  private final JobService jobService;

  public SchedulerImpl(ZooKeeper zooKeeper) {
    jobService = new JobServiceImpl(zooKeeper);
  }

  @Override
  public JobDefinition runOnce(Class<? extends Job> jobClass, Instant time) {
    JobDefinition jobDef = JobDefinition.newBuilder(jobClass).startAt(time).build();
    schedule(jobDef);
    return jobDef;
  }

  @Override
  public JobDefinition runNow(Class<? extends Job> jobClass) {
    JobDefinition jobDef = JobDefinition.newBuilder(jobClass).build();
    schedule(jobDef);
    return jobDef;
  }

  @Override
  public void schedule(JobDefinition job) {
    wrapException(() -> jobService.saveJobDefinition(job));
  }

  @Override
  public JobDefinition pause(UUID id, boolean mayInterrupt) {
    return wrapException(() -> jobService.pauseJob(id, mayInterrupt));
  }

  @Override
  public JobDefinition resume(UUID id) {
    return wrapException(() -> jobService.resumeJob(id));
  }

  @Override
  public List<JobDefinition> listJobDefinitionsByName(String name) {
    return wrapException(() -> jobService.listJobDefinitionsByName(name));
  }

  @Override
  public List<JobDefinition> listJobDefinitions() {
    return wrapException(jobService::listAllJobDefinitions);
  }

  @Override
  public Map<JobDefinition, JobDefinitionStatus> listJobDefinitionsWithStatus() {
    return wrapException(jobService::listJobDefinitionsWithStatus);
  }

  @Override
  public JobDefinition delete(UUID id) {
    return wrapException(
        () ->
            jobService
                .getJobDefinitionByIdAsync(id)
                .thenCompose(job -> jobService.deleteJobDefinitionAsync(job).thenApply(v -> job))
                .get());
  }

  <T> T wrapException(Executable<T> callable) {
    try {
      return callable.execute();
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new JobException(e);
    }
  }

  @FunctionalInterface
  interface Executable<T> {
    T execute() throws Throwable;
  }
}
