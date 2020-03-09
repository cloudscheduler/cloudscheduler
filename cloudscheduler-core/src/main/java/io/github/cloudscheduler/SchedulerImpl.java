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
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduler implementation.
 *
 * @author Wei Gao
 */
public class SchedulerImpl implements Scheduler {
  private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);

  private final JobService jobService;

  public SchedulerImpl(ZooKeeper zooKeeper) {
    jobService = new JobServiceImpl(() -> zooKeeper);
  }

  public SchedulerImpl(Supplier<ZooKeeper> zooKeeperSupplier) {
    jobService = new JobServiceImpl(zooKeeperSupplier);
  }

  @Override
  public JobDefinition runOnce(Class<? extends Job> jobClass, Instant time) throws JobException {
    JobDefinition jobDef = JobDefinition.newBuilder(jobClass)
        .startAt(time)
        .build();
    schedule(jobDef);
    return jobDef;
  }

  @Override
  public JobDefinition runNow(Class<? extends Job> jobClass) throws JobException {
    JobDefinition jobDef = JobDefinition.newBuilder(jobClass)
        .build();
    schedule(jobDef);
    return jobDef;
  }

  @Override
  public void schedule(JobDefinition job) throws JobException {
    try {
      jobService.saveJobDefinition(job);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new JobException(cause);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new JobException(e);
    }
  }

  @Override
  public JobDefinition pause(UUID id, boolean mayInterrupt) throws JobException {
    try {
      return jobService.pauseJob(id, mayInterrupt);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new JobException(cause);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new JobException(e);
    }
  }

  @Override
  public JobDefinition resume(UUID id) throws JobException {
    try {
      return jobService.resumeJob(id);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new JobException(cause);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new JobException(e);
    }
  }

  @Override
  public List<JobDefinition> listJobDefinitionsByName(String name) {
    try {
      return jobService.listJobDefinitionsByName(name);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new JobException(cause);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new JobException(e);
    }
  }

  @Override
  public List<JobDefinition> listJobDefinitions() {
    try {
      return jobService.listAllJobDefinitions();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new JobException(cause);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new JobException(e);
    }
  }

  @Override
  public Map<JobDefinition, JobDefinitionStatus> listJobDefinitionsWithStatus() {
    try {
      return jobService.listJobDefinitionsWithStatusAsync().get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new JobException(cause);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new JobException(e);
    }
  }

  @Override
  public JobDefinition delete(UUID id) {
    try {
      return jobService.getJobDefinitionByIdAsync(id).thenCompose(job ->
          jobService.deleteJobDefinitionAsync(job).thenApply(v -> job)).get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new JobException(cause);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new JobException(e);
    }
  }
}
