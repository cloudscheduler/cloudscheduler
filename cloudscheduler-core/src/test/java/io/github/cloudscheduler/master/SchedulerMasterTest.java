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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cloudscheduler.AbstractCloudSchedulerObserver;
import io.github.cloudscheduler.AbstractTest;
import io.github.cloudscheduler.CloudSchedulerObserver;
import io.github.cloudscheduler.JobFactory;
import io.github.cloudscheduler.JobScheduleCalculator;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.SimpleJobFactory;
import io.github.cloudscheduler.TestJob;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionState;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Wei Gao */
public class SchedulerMasterTest extends AbstractTest {

  private static final Logger logger = LoggerFactory.getLogger(SchedulerMasterTest.class);
  private final JobFactory jobFactory = new SimpleJobFactory();

  @Test
  @Timeout(2)
  public void testStartSchedulerMaster() throws Throwable {
    CountDownLatch jobInScheduledCounter = new CountDownLatch(1);
    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
                jobInScheduledCounter.countDown();
              }
            });
    master.start();
    try {
      JobDefinition job = JobDefinition.newBuilder(TestJob.class).build();
      jobService.saveJobDefinition(job);
      jobInScheduledCounter.await();
      JobDefinitionProcessor processor = master.getJobProcessorById(job.getId());
      assertThat(processor).isNotNull();
    } finally {
      master.shutdown();
    }
  }

  @Test
  @Timeout(3)
  public void testMultipleJobDefinition() throws Throwable {
    final AtomicReference<CountDownLatch> jobInScheduledCounter =
        new AtomicReference<>(new CountDownLatch(1));
    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
                jobInScheduledCounter.get().countDown();
              }
            });
    master.start();
    try {
      JobDefinition job = JobDefinition.newBuilder(TestJob.class).build();
      jobService.saveJobDefinition(job);
      UUID jobId1 = job.getId();
      jobInScheduledCounter.get().await();
      jobInScheduledCounter.set(new CountDownLatch(1));
      JobDefinitionProcessor processor = master.getJobProcessorById(jobId1);
      assertThat(processor).isNotNull();
      job = JobDefinition.newBuilder(TestJob.class).build();
      jobService.saveJobDefinition(job);
      UUID jobId2 = job.getId();
      jobInScheduledCounter.get().await();
      processor = master.getJobProcessorById(jobId1);
      assertThat(processor).isNotNull();
      processor = master.getJobProcessorById(jobId2);
      assertThat(processor).isNotNull();
    } finally {
      master.shutdown();
    }
  }

  @Test
  @Timeout(2)
  public void testScheduleNowJob() throws Throwable {
    CountDownLatch jobDefFinishedCounter = new CountDownLatch(1);
    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
                logger.info("Job instance scheduled");
              }

              @Override
              public void jobDefinitionCompleted(UUID id, Instant time) {
                logger.info("Job definition finished");
                jobDefFinishedCounter.countDown();
              }
            });
    master.start();
    try {
      JobDefinition job = JobDefinition.newBuilder(TestJob.class).build();

      jobService.saveJobDefinition(job);
      JobDefinitionStatus jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.CREATED);

      assertThat(jobDefinitionStatus.getLastScheduleTime());

      jobDefFinishedCounter.await();

      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.FINISHED);

      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNotNull();

      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
    } finally {
      master.shutdown();
    }
  }

  @Test
  @Timeout(30)
  public void testScheduleJobWithRepeat() throws Throwable {
    final CountDownLatch masterUpCounter = new CountDownLatch(1);
    final AtomicReference<CountDownLatch> jobInstanceScheduledCounter =
        new AtomicReference<>(new CountDownLatch(1));
    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void masterNodeUp(UUID nodeId, Instant time) {
                masterUpCounter.countDown();
              }

              @Override
              public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
                jobInstanceScheduledCounter.get().countDown();
              }
            });
    master.start();
    masterUpCounter.await();
    try {
      JobDefinition job =
          JobDefinition.newBuilder(TestJob.class)
              .initialDelay(Duration.ofSeconds(5))
              .fixedRate(Duration.ofSeconds(5))
              .allowDupInstances()
              .repeat(3)
              .build();
      jobService.saveJobDefinition(job);
      JobDefinitionStatus jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.CREATED);
      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNull();

      jobInstanceScheduledCounter.get().await();
      jobInstanceScheduledCounter.set(new CountDownLatch(1));

      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(is).hasSize(1);
      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNotNull();

      jobInstanceScheduledCounter.get().await();
      jobInstanceScheduledCounter.set(new CountDownLatch(1));
      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(2);

      jobInstanceScheduledCounter.get().await();
      jobInstanceScheduledCounter.set(new CountDownLatch(1));
      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(3);

      assertThat(jobInstanceScheduledCounter.get().await(6000L, TimeUnit.MILLISECONDS))
          .as("There shouldn't be another job instance scheduled")
          .isFalse();
      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(3);

      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.FINISHED);
    } finally {
      master.shutdown();
    }
  }

  @Test
  @Timeout(3)
  public void testScheduleJobAt() throws Throwable {
    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch jobDefFinishedCounter = new CountDownLatch(1);
    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void masterNodeUp(UUID nodeId, Instant time) {
                masterUpCounter.countDown();
              }

              @Override
              public void jobDefinitionCompleted(UUID id, Instant time) {
                jobDefFinishedCounter.countDown();
              }
            });
    master.start();
    masterUpCounter.await();
    try {
      Instant t = Instant.now().plus(Duration.ofSeconds(1));
      JobDefinition job = JobDefinition.newBuilder(TestJob.class).startAt(t).build();
      jobService.saveJobDefinition(job);
      JobDefinitionStatus jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.CREATED);

      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).isEmpty();
      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNull();

      jobDefFinishedCounter.await();

      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.FINISHED);

      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNotNull();

      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
    } finally {
      master.shutdown();
    }
  }

  @Test
  @Timeout(3)
  public void testCustomizedScheduleTimeCalculator() throws Throwable {
    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch jobDefFinishedCounter = new CountDownLatch(1);
    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void masterNodeUp(UUID nodeId, Instant time) {
                masterUpCounter.countDown();
              }

              @Override
              public void jobDefinitionCompleted(UUID id, Instant time) {
                jobDefFinishedCounter.countDown();
              }
            });
    master.start();
    masterUpCounter.await();
    try {
      JobDefinition job =
          JobDefinition.newBuilder(TestJob.class)
              .customizedCalculator(TestScheduleCalculator.class)
              .build();
      jobService.saveJobDefinition(job);
      JobDefinitionStatus jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.CREATED);

      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).isEmpty();
      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNull();

      jobDefFinishedCounter.await();

      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.FINISHED);

      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNotNull();

      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
    } finally {
      master.shutdown();
    }
  }

  @Test
  @Timeout(30)
  public void testScheduleJobRepeatWithComplete() throws Throwable {
    final AtomicReference<CountDownLatch> jobInScheduledCounter =
        new AtomicReference<>(new CountDownLatch(1));
    final CountDownLatch jobDefFinishedCounter = new CountDownLatch(1);

    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
                jobInScheduledCounter.get().countDown();
              }

              @Override
              public void jobDefinitionCompleted(UUID id, Instant time) {
                jobDefFinishedCounter.countDown();
              }
            });
    master.start();
    try {
      UUID nodeId = UUID.randomUUID();

      JobDefinition job =
          JobDefinition.newBuilder(TestJob.class)
              .initialDelay(Duration.ofSeconds(5))
              .fixedRate(Duration.ofSeconds(5))
              .repeat(3)
              .build();
      jobService.saveJobDefinition(job);
      logger.info("Job definition created");
      JobDefinitionStatus jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.CREATED);
      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNull();

      jobInScheduledCounter.get().await();
      jobInScheduledCounter.set(new CountDownLatch(1));
      logger.info("Job instance scheduled");

      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(is).hasSize(1);
      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNotNull();
      assertThat(jobDefinitionStatus.getRunCount()).isEqualTo(1);

      assertThat(jobInScheduledCounter.get().await(6000L, TimeUnit.MILLISECONDS))
          .as("Job instance shouldn't scheduled again.")
          .isFalse();
      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getRunCount()).isEqualTo(1);

      JobInstance instance = is.iterator().next();
      logger.info("Start process JobInstance {}", instance.getId());
      jobService.startProcessJobInstance(instance.getId(), nodeId);
      logger.info("Complete process JobInstance {}", instance.getId());
      jobService.completeJobInstance(instance.getId(), nodeId, JobInstanceState.COMPLETE);

      jobInScheduledCounter.get().await();
      jobInScheduledCounter.set(new CountDownLatch(1));
      logger.info("Job instance scheduled again");

      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getRunCount()).isEqualTo(2);

      instance = is.iterator().next();
      logger.info("Start process JobInstance {}", instance.getId());
      jobService.startProcessJobInstance(instance.getId(), nodeId);
      logger.info("Complete process JobInstance {}", instance.getId());
      jobService.completeJobInstance(instance.getId(), nodeId, JobInstanceState.COMPLETE);

      jobDefFinishedCounter.await();
      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getRunCount()).isEqualTo(3);
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.FINISHED);
    } finally {
      master.shutdown();
    }
  }

  @Test
  @Timeout(30)
  public void testScheduleJobRepeatWithFixedDelay() throws Throwable {
    logger.info("Start fixed delay test.");
    final AtomicReference<CountDownLatch> jobInScheduledCounter =
        new AtomicReference<>(new CountDownLatch(1));
    final AtomicReference<CountDownLatch> jobInRemovedCounter =
        new AtomicReference<>(new CountDownLatch(1));
    final CountDownLatch jobDefFinishedCounter = new CountDownLatch(1);
    final AtomicInteger jobScheduledTimes = new AtomicInteger(0);

    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
                jobScheduledTimes.incrementAndGet();
                jobInScheduledCounter.get().countDown();
              }

              @Override
              public void jobInstanceRemoved(UUID jobDefId, UUID jobInId, Instant time) {
                jobInRemovedCounter.get().countDown();
              }

              @Override
              public void jobDefinitionCompleted(UUID id, Instant time) {
                jobDefFinishedCounter.countDown();
              }
            });
    master.start();
    try {
      UUID nodeId = UUID.randomUUID();

      JobDefinition job =
          JobDefinition.newBuilder(TestJob.class)
              .startAt(Instant.now().plusSeconds(2L))
              .fixedDelay(Duration.ofSeconds(5))
              .repeat(3)
              .build();
      jobService.saveJobDefinition(job);
      JobDefinitionStatus jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.CREATED);
      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNull();

      assertThat(jobInScheduledCounter.get().await(3L, TimeUnit.SECONDS))
          .as("Job should scheduled")
          .isTrue();
      jobInScheduledCounter.set(new CountDownLatch(1));
      logger.info("Job instance scheduled");

      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(is).hasSize(1);
      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNotNull();
      assertThat(jobDefinitionStatus.getRunCount()).isEqualTo(1);

      assertThat(jobInScheduledCounter.get().await(6L, TimeUnit.SECONDS))
          .as("Job instance shouldn't scheduled again.")
          .isFalse();
      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getRunCount()).isEqualTo(1);

      JobInstance instance = is.iterator().next();
      logger.info("Start process JobInstance {}", instance.getId());
      jobService.startProcessJobInstance(instance.getId(), nodeId);
      Thread.sleep(500L);
      logger.info("Complete process JobInstance {}", instance.getId());
      jobService.completeJobInstance(instance.getId(), nodeId, JobInstanceState.COMPLETE);

      jobInRemovedCounter.get().await();
      jobInRemovedCounter.set(new CountDownLatch(1));

      assertThat(jobInScheduledCounter.get().await(6L, TimeUnit.SECONDS))
          .as("Job should scheduled again.")
          .isTrue();
      jobInScheduledCounter.set(new CountDownLatch(1));
      logger.info("Job instance scheduled again");
      Thread.sleep(1000L);

      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobScheduledTimes.get())
          .as("Job scheduled should be called two times")
          .isEqualTo(2);
      assertThat(jobDefinitionStatus.getRunCount()).isEqualTo(2);

      instance = is.iterator().next();
      logger.info("Start process JobInstance {}", instance.getId());
      jobService.startProcessJobInstance(instance.getId(), nodeId);
      Thread.sleep(500L);
      logger.info("Complete process JobInstance {}", instance.getId());
      jobService.completeJobInstance(instance.getId(), nodeId, JobInstanceState.COMPLETE);

      jobInRemovedCounter.get().await();
      jobInRemovedCounter.set(new CountDownLatch(1));

      jobInScheduledCounter.get().await();
      jobInScheduledCounter.set(new CountDownLatch(1));
      logger.info("Job instance scheduled third time");

      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);

      instance = is.iterator().next();
      logger.info("Start process JobInstance {}", instance.getId());
      jobService.startProcessJobInstance(instance.getId(), nodeId);
      Thread.sleep(500L);
      logger.info("Complete process JobInstance {}", instance.getId());
      jobService.completeJobInstance(instance.getId(), nodeId, JobInstanceState.COMPLETE);

      jobDefFinishedCounter.await();

      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).isEmpty();
      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getRunCount()).isEqualTo(3);
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.FINISHED);
    } finally {
      master.shutdown();
    }
  }

  @Test
  @Timeout(20)
  public void testStart1000JobInstancesAtSameTime() throws Throwable {
    int jobDefNumber = 1000;
    final CountDownLatch jobInScheduledCounter = new CountDownLatch(jobDefNumber);
    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
                jobInScheduledCounter.countDown();
              }
            });
    master.start();
    try {
      Instant startTime = Instant.now().plusSeconds(5);
      List<CompletableFuture<?>> fs = new ArrayList<>(jobDefNumber);
      for (int i = 0; i < jobDefNumber; i++) {
        JobDefinition jobDef = JobDefinition.newBuilder(TestJob.class).startAt(startTime).build();
        fs.add(
            jobService
                .saveJobDefinitionAsync(jobDef)
                .thenAccept(v -> logger.info("JobDefinition {} saved", jobDef.getId())));
      }
      CountDownLatch countDownLatch = new CountDownLatch(1);
      CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
          .thenAccept(v -> countDownLatch.countDown());
      countDownLatch.await();
      logger.info("All job definition created.");
      jobInScheduledCounter.await();
      logger.info("All job instance scheduled.");
      List<JobInstance> jobIns = jobService.listAllJobInstances();
      assertThat(jobIns).hasSize(jobDefNumber);
    } finally {
      master.shutdown();
    }
  }

  @Test
  @Timeout(30)
  public void test1000WorkerNodesWith1000JobDefs() throws Throwable {
    int jobDefNumber = 1000;
    int nodeNumber = 1000;

    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch workerUpCounter = new CountDownLatch(nodeNumber);
    CountDownLatch jobInstanceCounter = new CountDownLatch(jobDefNumber);

    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void masterNodeUp(UUID nodeId, Instant time) {
                masterUpCounter.countDown();
              }

              @Override
              public void workerNodeUp(UUID nodeId, Instant time) {
                workerUpCounter.countDown();
              }

              @Override
              public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
                jobInstanceCounter.countDown();
              }
            });
    Instant startTime = Instant.now().plusSeconds(5);
    List<CompletableFuture<?>> fs = new ArrayList<>(jobDefNumber);
    for (int i = 0; i < jobDefNumber; i++) {
      JobDefinition jobDef = JobDefinition.newBuilder(TestJob.class).startAt(startTime).build();
      fs.add(
          jobService
              .saveJobDefinitionAsync(jobDef)
              .thenAccept(v -> logger.info("JobDefinition {} saved", jobDef.getId())));
    }
    for (int i = 0; i < nodeNumber; i++) {
      Node node = new Node();
      fs.add(
          jobService
              .registerWorkerAsync(node)
              .thenAccept(v -> logger.info("Node: {} registered", node)));
    }
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
        .thenAccept(v -> countDownLatch.countDown());
    master.start();
    masterUpCounter.await();
    try {
      countDownLatch.await();
      jobInstanceCounter.await();
      List<JobInstance> jobIns = jobService.listAllJobInstances();
      assertThat(jobIns).hasSize(jobDefNumber);
    } finally {
      master.shutdown();
    }
  }

  @Test
  @Timeout(5)
  public void testScheduleJobAtWhenMasterShutdown() throws Throwable {
    final CountDownLatch jobInScheduledCounter = new CountDownLatch(1);
    final CountDownLatch jobDefFinishedCounter = new CountDownLatch(1);
    CloudSchedulerObserver observer =
        new AbstractCloudSchedulerObserver() {
          @Override
          public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
            jobInScheduledCounter.countDown();
          }

          @Override
          public void jobDefinitionCompleted(UUID id, Instant time) {
            jobDefFinishedCounter.countDown();
          }
        };
    SchedulerMaster master =
        new SchedulerMaster(new Node(), zkUrl, Integer.MAX_VALUE, jobFactory, threadPool, observer);
    SchedulerMaster master2 =
        new SchedulerMaster(new Node(), zkUrl, Integer.MAX_VALUE, jobFactory, threadPool, observer);

    master.start();
    master2.start();
    try {
      Instant t = Instant.now().plus(Duration.ofSeconds(3));
      JobDefinition job = JobDefinition.newBuilder(TestJob.class).startAt(t).build();
      jobService.saveJobDefinition(job);
      JobDefinitionStatus jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.CREATED);

      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).isEmpty();
      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNull();

      master.shutdown();
      assertThat(jobInScheduledCounter.await(3500L, TimeUnit.MILLISECONDS))
          .as("Job instance should be scheduled within 3 seconds")
          .isTrue();
      assertThat(jobDefFinishedCounter.await(1000L, TimeUnit.MILLISECONDS))
          .as("Job definition should finished.")
          .isTrue();

      jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.FINISHED);

      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNotNull();

      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
    } finally {
      master2.shutdown();
    }
  }

  @Test
  @Timeout(15)
  public void testGlobalRepeatJobs() throws Throwable {
    int numberOfWorkers = 5;
    logger.info("Start five workers");
    List<CompletableFuture<?>> fs = new ArrayList<>(numberOfWorkers);
    List<UUID> workerIds = new ArrayList<>(numberOfWorkers);
    for (int i = 0; i < numberOfWorkers; i++) {
      Node node = new Node();
      workerIds.add(node.getId());
      fs.add(
          jobService
              .registerWorkerAsync(node)
              .thenAccept(v -> logger.info("Node: {} registered", node)));
    }
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
        .thenAccept(v -> countDownLatch.countDown());
    countDownLatch.await();

    final AtomicReference<CountDownLatch> jobInScheduledCounter =
        new AtomicReference<>(new CountDownLatch(1));
    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
                jobInScheduledCounter.get().countDown();
              }
            });
    logger.info("Start master");
    master.start();
    try {
      Instant t = Instant.now().plus(Duration.ofSeconds(3));
      logger.info("Create job definition");
      JobDefinition job =
          JobDefinition.newBuilder(TestJob.class)
              .startAt(t)
              .global()
              .fixedDelay(Duration.ofSeconds(5))
              .repeat(3)
              .build();
      jobService.saveJobDefinition(job);
      logger.info("Make sure JobDefinition status is right.");
      JobDefinitionStatus jobDefinitionStatus = jobService.getJobStatusById(job.getId());
      assertThat(jobDefinitionStatus.getState()).isEqualTo(JobDefinitionState.CREATED);

      logger.info("Make sure job instance not scheduled.");
      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).isEmpty();
      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNull();

      assertThat(jobInScheduledCounter.get().await(3500L, TimeUnit.MILLISECONDS))
          .as("Should scheduled within 3.5 seconds")
          .isTrue();
      jobInScheduledCounter.set(new CountDownLatch(1));

      logger.info("JobInstance should be scheduled now");
      jobDefinitionStatus = jobService.getJobStatusById(job.getId());

      assertThat(jobDefinitionStatus.getLastScheduleTime()).isNotNull();

      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
      JobInstance jobIn = is.iterator().next();
      assertThat(jobIn.getRunStatus()).hasSize(5);

      logger.info("Start process jobs.");
      for (UUID workerId : workerIds) {
        jobService.startProcessJobInstance(jobIn.getId(), workerId);
      }
      logger.info("Complete jobs");
      for (UUID workerId : workerIds) {
        jobService.completeJobInstance(jobIn.getId(), workerId, JobInstanceState.COMPLETE);
      }
      Thread.sleep(500L);
      logger.info("Make sure JobInstance cleaned up.");
      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).isEmpty();

      assertThat(jobInScheduledCounter.get().await(3000L, TimeUnit.MILLISECONDS))
          .as("Shouldn't scheduled in 3 seconds")
          .isFalse();
      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).isEmpty();

      assertThat(jobInScheduledCounter.get().await(2000L, TimeUnit.MILLISECONDS))
          .as("Should schedled after another 2 seconds")
          .isTrue();
      is = jobService.getJobInstancesByJobDef(job);
      assertThat(is).hasSize(1);
      jobIn = is.iterator().next();
      assertThat(jobIn.getRunStatus()).hasSize(5);
    } finally {
      master.shutdown();
    }
  }

  public static class TestScheduleCalculator implements JobScheduleCalculator {
    @Override
    public Instant calculateNextRunTime(JobDefinition jobDefinition, JobDefinitionStatus status) {
      if (status.getRunCount() == 0) {
        logger.info("Calculate for first time, delay 1.5 seconds");
        return Instant.now().plusMillis(1500L);
      } else {
        logger.info("Calculate for more than 1 times, should stop  now.");
        return null;
      }
    }
  }
}
