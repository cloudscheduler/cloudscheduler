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

package io.github.cloudscheduler.master;

import io.github.cloudscheduler.AsyncService;
import io.github.cloudscheduler.CloudSchedulerObserver;
import io.github.cloudscheduler.EventType;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionState;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import io.github.cloudscheduler.service.JobService;
import io.github.cloudscheduler.util.CompletableFutureUtils;
import io.github.cloudscheduler.util.CronExpression;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job definition processor, used to process job definitions.
 *
 * @author Wei Gao
 */
class JobDefinitionProcessor implements AsyncService {
  private static final Logger logger = LoggerFactory.getLogger(JobDefinitionProcessor.class);

  private final JobDefinition jobDef;
  private final ExecutorService threadPool;
  private final JobService jobService;
  private final Consumer<EventType> statusListener;
  private final CronExpression cronExpression;
  private final AtomicBoolean running;
  private final CloudSchedulerObserver observer;
  private final AtomicBoolean scheduled;
  private CompletableFuture<?> scheduledFuture;
  private CompletableFuture<?> listenerJob;
  private JobDefinitionState jobDefState;

  JobDefinitionProcessor(JobDefinition jobDef,
                         ExecutorService threadPool,
                         JobService jobService,
                         CloudSchedulerObserver observer) {
    this.jobDef = jobDef;
    if (jobDef.getCron() != null) {
      cronExpression = CronExpression.createWithoutSeconds(jobDef.getCron());
    } else {
      cronExpression = null;
    }
    this.threadPool = threadPool;
    this.jobService = jobService;
    this.running = new AtomicBoolean(false);
    this.scheduled = new AtomicBoolean(false);
    this.observer = observer;
    listenerJob = CompletableFuture.completedFuture(null);
    statusListener = event -> {
      if (running.get()) {
        switch (event) {
          case ENTITY_UPDATED:
            logger.trace("Status entity updated.");
            onStatusChanged();
            break;
          default:
            break;
        }
      }
    };
  }

  void start() {
    if (running.compareAndSet(false, true)) {
      logger.info("Start JobDefinition processor for id: {}", jobDef.getId());
      // Schedule next job instance
      // Monitor status
      onStatusChanged();
    }
  }

  public CompletableFuture<Void> shutdownAsync() {
    if (running.compareAndSet(true, false)) {
      logger.info("Shutdown JobDefinition processor for id: {}", jobDef.getId());
      return listenerJob.exceptionally(e -> {
        logger.debug("Error happened in status listener", e);
        return null;
      }).thenAccept(v -> {
        if (scheduledFuture != null) {
          if (scheduledFuture.cancel(true)) {
            logger.debug("Scheduled future got cancelled.");
          } else {
            logger.debug("Scheduled future already done or already cancelled.");
          }
        }
      }).whenComplete((v, cause) -> logger.info("Job definition processor down.", jobDef.getId()))
          .thenApply(v -> null);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  private CompletableFuture<Void> scheduleNextJobInstance() {
    logger.trace("Scheduling next job instance for job definition: {}.", jobDef.getId());
    return jobService.getJobStatusByIdAsync(jobDef.getId()).thenComposeAsync(status ->
        calculateNextRunTime(jobDef, status)
            .thenCompose(time -> {
              if (time == null) {
                return CompletableFuture.completedFuture(null);
              } else {
                Duration d = Duration.between(Instant.now(), time);
                if (d.isZero() || d.isNegative()) {
                  return scheduleJobInstance();
                } else {
                  if (scheduled.compareAndSet(false, true)) {
                    logger.trace("Schedule scheduleJobInstance after: {}", d);
                    scheduledFuture = CompletableFutureUtils.completeAfter(d, null).thenCompose(
                        n -> {
                          logger.trace("Time to schedule job instance.");
                          scheduled.set(false);
                          return scheduleJobInstance();
                        });
                  }
                  return CompletableFuture.completedFuture(null);
                }
              }
            }), threadPool).thenApply(v -> null);
  }

  private CompletableFuture<JobInstance> scheduleJobInstance() {
    logger.trace("Processing create job instance.");
    return jobService.getJobStatusByIdAsync(jobDef.getId()).thenComposeAsync(status -> {
      boolean completed = true;
      for (Map.Entry<UUID, JobInstanceState> entry : status.getJobInstanceState()
          .entrySet()) {
        if (!entry.getValue().isComplete(jobDef.isGlobal())) {
          completed = false;
          break;
        }
      }
      if (completed || jobDef.isAllowDupInstances()) {
        logger.trace("Creating JobInstance");
        return jobService.scheduleJobInstanceAsync(jobDef).thenApplyAsync(ji -> {
          observer.jobInstanceScheduled(jobDef.getId(), ji.getId(), Instant.now());
          return ji;
        }, threadPool);
      } else {
        logger.trace("Previous JobInstance not complete, will not create JobInstance");
        return CompletableFuture.completedFuture(null);
      }
    }, threadPool).whenComplete((v, cause) -> {
      if (cause != null) {
        logger.error("Error happened when schedule job instance.", cause);
      }
      if (jobDef.getDelay() == null) {
        logger.trace("Schedule next time");
        listenerJob = listenerJob.thenCompose(n -> scheduleNextJobInstance());
      }
    });
  }

  private void onStatusChanged() {
    logger.debug("JobDefinition status changed for id: {}", jobDef.getId());
    if (running.get()) {
      listenerJob = listenerJob.thenCombine(jobService
          .getJobStatusByIdAsync(jobDef.getId(), statusListener)
          .thenComposeAsync(
              status -> {
                AtomicBoolean started = new AtomicBoolean(false);
                if (jobDefState == null) {
                  // started
                  started.set(true);
                } else if (!jobDefState.equals(status.getState())) {
                  if (JobDefinitionState.CREATED.equals(status.getState())) {
                    logger.trace("Job {} got resumed.", jobDef.getId());
                    observer.jobDefinitionResumed(jobDef.getId(), Instant.now());
                    started.set(true);
                  } else if (jobDefState.equals(JobDefinitionState.CREATED)
                      && status.getState().equals(JobDefinitionState.PAUSED)) {
                    logger.trace("Job {} got paused.", jobDef.getId());
                    if (scheduledFuture != null) {
                      logger.trace("Cancel scheduler");
                      if (scheduledFuture.cancel(true)) {
                        logger.debug("Scheduled future got cancelled");
                        scheduled.set(false);
                      } else {
                        logger.debug("Scheduled future is done or already cancelled");
                      }
                    }
                    // paused
                    observer.jobDefinitionPaused(jobDef.getId(), Instant.now());
                  }
                }
                jobDefState = status.getState();
                return jobService.cleanUpJobInstances(jobDef)
                    .thenAcceptAsync(v -> {
                      logger.debug("{} job instance removed.", v.size());
                      if (!v.isEmpty()) {
                        Instant now = Instant.now();
                        for (JobInstance jobIn : v) {
                          observer.jobInstanceRemoved(jobDef.getId(), jobIn.getId(), now);
                        }
                      }
                    }, threadPool)
                    .exceptionally(e -> {
                      logger.error("Error happened when cleanup JobInstance of JobDefinition: {}",
                          jobDef, e);
                      return null;
                    })
                    // FIXME: Should we load JobDefinitionStatus again?
                    // In case after we load and set listener, status changed, this may
                    // cause schedule again?
                    // Also, cleanup job instance more like just remove cleanup, shouldn't
                    // Change the logic of detect job instance complete.
                    //     .thenCompose(v -> jobService.getJobStatusByIdAsync(jobDef.getId()))
                    .thenCompose(v -> {
                      if (jobDefState.isActive()) {
                        boolean completed = true;
                        for (JobInstanceState s : status.getJobInstanceState().values()) {
                          if (!s.isComplete(jobDef.isGlobal())) {
                            completed = false;
                          }
                        }
                        if (started.get() || (completed && jobDef.getDelay() != null
                            && !scheduled.get())) {
                          logger.trace("Schedule next job instance");
                          return scheduleNextJobInstance();
                        }
                      }
                      return CompletableFuture.completedFuture(null);
                    });
              },
              threadPool), (a, b) -> null);
    }
  }

  /**
   * Calculate JobDefinition next runtime.
   *
   * @param jobDef JobDefinition
   * @param status JobDefinition status
   * @return Next runtime, {@code null} if Job repeat or end time reached,
   *     no previous job instance not complete and not allow duplicate job instance
   */
  private CompletableFuture<Instant> calculateNextRunTime(JobDefinition jobDef,
                                                          JobDefinitionStatus status) {
    logger.trace("Calculating next runtime for JobDefinition: {}", jobDef.getId());
    if (status.getState().isActive()) {
      Instant now = Instant.now();
      logger.trace("JobDefinition is alive");
      if ((jobDef.getRepeat() != null && status.getRunCount() >= jobDef.getRepeat())) {
        logger.trace("Job run count exceed repeat time, no next runtime");
        return completeJobDefinition(jobDef);
      }
      if (jobDef.getMode().equals(JobDefinition.ScheduleMode.CRON)) {
        logger.trace("Cron job");
        CronExpression cron;
        if (cronExpression != null) {
          cron = cronExpression;
        } else {
          cron = CronExpression.createWithoutSeconds(jobDef.getCron());
        }
        try {
          Instant nextTime = cron.nextTimeAfter(ZonedDateTime.now()).toInstant();
          if (jobDef.getEndTime() != null && jobDef.getEndTime().isBefore(nextTime)) {
            logger.trace("Next runtime {} after end time {}, no next runtime",
                nextTime, jobDef.getEndTime());
            return completeJobDefinition(jobDef);
          }
          logger.trace("Next runtime should be: {}", nextTime);
          return CompletableFuture.completedFuture(nextTime);
        } catch (IllegalArgumentException e) {
          logger.debug("Got IllegalArgumentException, no next runtime", e);
          return completeJobDefinition(jobDef);
        }
      } else {
        Duration interval;
        Instant nextTime;
        if (jobDef.getDelay() != null) {
          interval = jobDef.getDelay();
          nextTime = status.getLastCompleteTime();
          if (nextTime == null) {
            nextTime = jobDef.getStartTime();
          }
          if (nextTime == null) {
            nextTime = now;
          }
        } else if (jobDef.getRate() != null) {
          interval = jobDef.getRate();
          nextTime = status.getLastScheduleTime();
          if (nextTime == null) {
            nextTime = jobDef.getStartTime();
          }
          if (nextTime == null) {
            nextTime = now;
          }
        } else if (status.getLastScheduleTime() == null) {
          nextTime = jobDef.getStartTime();
          if (nextTime == null) {
            nextTime = now;
          }
          logger.trace("Next runtime should be: {}", nextTime);
          return CompletableFuture.completedFuture(nextTime);
        } else {
          logger.trace("No delay, no rate, there will be no next runtime");
          return completeJobDefinition(jobDef);
        }
        while (nextTime.isBefore(now)) {
          nextTime = nextTime.plus(interval);
        }
        if (jobDef.getEndTime() != null && jobDef.getEndTime().isBefore(nextTime)) {
          logger.trace("Next runtime {} after end time {}, no next runtime",
              nextTime, jobDef.getEndTime());
          return completeJobDefinition(jobDef);
        }
        logger.trace("Next runtime should be: {}", nextTime);
        return CompletableFuture.completedFuture(nextTime);
      }
    } else {
      logger.trace("JobDefinition is not active, no next runtime");
      return CompletableFuture.completedFuture(null);
    }
  }

  private CompletableFuture<Instant> completeJobDefinition(JobDefinition jobDef) {
    return jobService.completeJobDefinitionAsync(jobDef)
        .thenAcceptAsync(s -> observer.jobDefinitionCompleted(jobDef.getId(), Instant.now()),
            threadPool)
        .thenApply(n -> null);
  }
}
