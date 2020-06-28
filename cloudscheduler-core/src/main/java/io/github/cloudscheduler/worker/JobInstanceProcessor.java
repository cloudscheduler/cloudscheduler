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

import io.github.cloudscheduler.AsyncService;
import io.github.cloudscheduler.CloudSchedulerObserver;
import io.github.cloudscheduler.Job;
import io.github.cloudscheduler.JobExecutionContext;
import io.github.cloudscheduler.JobFactory;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import io.github.cloudscheduler.service.JobService;
import io.github.cloudscheduler.util.CompletableFutureUtils;
import io.github.cloudscheduler.util.lock.DistributedLock;
import io.github.cloudscheduler.util.lock.DistributedLockImpl;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job instance processor, used to process job instances.
 *
 * @author Wei Gao
 */
class JobInstanceProcessor implements AsyncService {
  private static final Logger logger = LoggerFactory.getLogger(JobInstanceProcessor.class);

  private final Node node;
  private final UUID jobInId;
  private final ZooKeeper zooKeeper;
  private final ExecutorService threadPool;
  private final ExecutorService customerThreadPool;
  private final JobService jobService;
  private final JobFactory jobFactory;
  private final CloudSchedulerObserver observer;
  private CompletableFuture<?> future;
  private DistributedLock lock;

  JobInstanceProcessor(
      Node node,
      JobInstance jobIn,
      ZooKeeper zooKeeper,
      ExecutorService threadPool,
      ExecutorService customerThreadPool,
      JobService jobService,
      JobFactory jobFactory,
      CloudSchedulerObserver observer) {
    this.node = node;
    this.jobInId = jobIn.getId();
    this.zooKeeper = zooKeeper;
    this.threadPool = threadPool;
    this.customerThreadPool = customerThreadPool;
    this.jobService = jobService;
    this.jobFactory = jobFactory;
    this.observer = observer;
  }

  void start() {
    logger.trace("Start JobInstance processor for {}", jobInId);
    future =
        jobService
            .getJobInstanceByIdAsync(jobInId)
            .thenCompose(
                jobIn ->
                    jobService
                        .getJobDefinitionByIdAsync(jobIn.getJobDefId())
                        .thenCompose(
                            jobDef -> {
                              logger.trace(
                                  "JobInstance {}, JobDefinition is: {}", jobInId, jobDef.getId());
                              if (jobDef.isGlobal()) {
                                if (jobIn.getRunStatus().keySet().contains(node.getId())) {
                                  return runJob(jobDef, jobIn);
                                } else {
                                  return CompletableFuture.completedFuture(null);
                                }
                              } else {
                                lock =
                                    new DistributedLockImpl(
                                        node.getId(),
                                        zooKeeper,
                                        jobService.getJobInstancePath(jobInId),
                                        null);
                                return lock.lock()
                                    .thenCompose(
                                        v -> {
                                          logger.trace("Loaded JobInstance for id: {}", jobInId);
                                          if (!jobIn.getJobState().isComplete(jobDef.isGlobal())) {
                                            logger.trace(
                                                "JobInstance {} not complete yet, run it", jobInId);
                                            return runJob(jobDef, jobIn);
                                          } else {
                                            logger.trace(
                                                "JobInstance {} completed, not run it.", jobInId);
                                            return CompletableFuture.completedFuture(null);
                                          }
                                        })
                                    .exceptionally(
                                        cause -> {
                                          logger.debug(
                                              "Error happened when processing job instance {}",
                                              jobInId,
                                              cause);
                                          return null;
                                        })
                                    .thenCompose(
                                        v -> {
                                          logger.trace("Unlock JobInstance: {}", jobInId);
                                          return lock.unlock();
                                        })
                                    .exceptionally(
                                        cause -> {
                                          logger.error(
                                              "Error happened when unlock distribute lock for JobInstance: {}",
                                              jobIn.getId(),
                                              cause);
                                          return null;
                                        });
                              }
                            }));
  }

  private CompletableFuture<Void> runJob(JobDefinition jobDef, JobInstance jobIn) {
    logger.debug("Run job instance");
    return jobService
        .getJobStatusByIdAsync(jobDef.getId())
        .thenCompose(
            status ->
                jobService
                    .startProcessJobInstanceAsync(jobIn.getId(), node.getId())
                    .thenApplyAsync(
                        ji -> {
                          observer.jobInstanceStarted(
                              jobDef.getId(), jobIn.getId(), node.getId(), Instant.now());
                          return ji;
                        },
                        threadPool)
                    .thenComposeAsync(
                        ji -> {
                          logger.trace("Preparing job");
                          try {
                            Job job = jobFactory.newJob(jobDef);
                            logger.trace("Created job object instance");
                            JobExecutionContext ctx =
                                new JobExecutionContextImpl(node, jobDef, status, jobIn);
                            logger.trace("Created job execution context, run it.");
                            job.execute(ctx);
                            logger.trace("Return from job implementation.");
                            return CompletableFuture.completedFuture((Void) null);
                          } catch (Throwable e) {
                            return CompletableFutureUtils.exceptionalCompletableFuture(e);
                          }
                        },
                        customerThreadPool))
        .handleAsync(
            (v, cause) -> {
              if (cause != null) {
                logger.debug(
                    "Error happened when run job instance: {}, class: {}",
                    jobIn.getId(),
                    jobDef.getClass(),
                    cause);
              }
              return cause == null ? JobInstanceState.COMPLETE : JobInstanceState.FAILED;
            },
            threadPool)
        .thenCompose(
            state -> {
              logger.trace(
                  "Completing JobInstance: {}, node: {}, state: {}",
                  jobIn.getId(),
                  node.getId(),
                  state);
              return jobService
                  .completeJobInstanceAsync(jobIn.getId(), node.getId(), state)
                  .whenCompleteAsync(
                      (v, cause) -> {
                        try {
                          if (JobInstanceState.COMPLETE.equals(state)) {
                            observer.jobInstanceCompleted(
                                jobDef.getId(), jobIn.getId(), node.getId(), Instant.now());
                          } else {
                            observer.jobInstanceFailed(
                                jobDef.getId(), jobIn.getId(), node.getId(), Instant.now());
                          }
                        } catch (Throwable e) {
                          logger.debug("Error when notify observer, ignore it.", e);
                        }
                      },
                      threadPool)
                  .thenApply(v -> null);
            });
  }

  public CompletableFuture<?> shutdownAsync() {
    CompletableFuture<?> f;
    if (future != null) {
      future.cancel(true);
      f =
          future.exceptionally(
              cause -> {
                logger.debug("Error happened during job running", cause);
                return null;
              });
    } else {
      f = CompletableFuture.completedFuture(null);
    }
    if (lock != null) {
      f = f.thenCompose(v -> lock.unlock());
    }
    return f;
  }
}
