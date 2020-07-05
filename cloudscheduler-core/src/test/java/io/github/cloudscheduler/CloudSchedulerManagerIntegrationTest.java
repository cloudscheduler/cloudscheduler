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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Wei Gao */
@Tag("integration")
public class CloudSchedulerManagerIntegrationTest extends AbstractTest {
  private static final Logger logger =
      LoggerFactory.getLogger(CloudSchedulerManagerIntegrationTest.class);

  private static final ConcurrentMap<String, CountDownLatch> counters = new ConcurrentHashMap<>();
  private static final String TEST_NAME = "name";

  @Test
  @Timeout(3)
  public void testStartMasterAndWorker() throws Throwable {
    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch workerUpCounter = new CountDownLatch(1);
    CloudSchedulerManager manager =
        CloudSchedulerManager.newBuilder(zkUrl)
            .setThreadPool(threadPool)
            .setNodeId(UUID.randomUUID())
            .setZkTimeout(300)
            .setObserverSupplier(
                () ->
                    new AbstractCloudSchedulerObserver() {
                      @Override
                      public void masterNodeUp(UUID nodeId, Instant time) {
                        masterUpCounter.countDown();
                      }

                      @Override
                      public void workerNodeUp(UUID nodeId, Instant time) {
                        workerUpCounter.countDown();
                      }
                    })
            .build();
    manager.start();
    try {
      masterUpCounter.await();
      workerUpCounter.await();
      List<UUID> workers = jobService.getCurrentWorkers();
      assertThat(workers).hasSize(1);

      logger.trace("ZooKeeper: {}", zooKeeper);
      List<String> masters = ZooKeeperUtils.getChildren(zooKeeper, "/lock/master").get();
      assertThat(masters.size()).isEqualTo(1);
    } finally {
      manager.shutdown();
    }
  }

  @Test
  @Timeout(3)
  public void testSingleJob() throws Throwable {
    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch workerUpCounter = new CountDownLatch(1);
    String name = "testSingleJob";
    counters.putIfAbsent(name, new CountDownLatch(1));
    CountDownLatch counter = counters.get(name);
    CloudSchedulerManager manager =
        CloudSchedulerManager.newBuilder(zkUrl)
            .setThreadPool(threadPool)
            .setRoles(NodeRole.MASTER, NodeRole.WORKER)
            .setJobFactory(new SimpleJobFactory())
            .setObserverSupplier(
                () ->
                    new AbstractCloudSchedulerObserver() {
                      @Override
                      public void masterNodeUp(UUID nodeId, Instant time) {
                        masterUpCounter.countDown();
                      }

                      @Override
                      public void workerNodeUp(UUID nodeId, Instant time) {
                        workerUpCounter.countDown();
                      }
                    })
            .build();
    manager.start();
    try {
      Scheduler scheduler = new SchedulerImpl(zooKeeper);
      masterUpCounter.await();
      workerUpCounter.await();

      JobDefinition jd =
          JobDefinition.newBuilder(CloudTestJob.class).jobData(TEST_NAME, name).runNow().build();
      scheduler.schedule(jd);
      assertThat(counter.await(1000L, TimeUnit.MILLISECONDS)).as("Job run be run.").isTrue();
    } finally {
      manager.shutdown();
    }
  }

  @Test
  public void testSingleRepeatJob() throws Throwable {
    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch workerUpCounter = new CountDownLatch(1);
    String name = "testSingleRepeatJob";
    counters.putIfAbsent(name, new CountDownLatch(3));
    CountDownLatch counter = counters.get(name);
    CloudSchedulerManager manager =
        CloudSchedulerManager.newBuilder(zkUrl)
            .setThreadPool(threadPool)
            .setObserverSupplier(
                () ->
                    new AbstractCloudSchedulerObserver() {
                      @Override
                      public void masterNodeUp(UUID nodeId, Instant time) {
                        masterUpCounter.countDown();
                      }

                      @Override
                      public void workerNodeUp(UUID nodeId, Instant time) {
                        workerUpCounter.countDown();
                      }
                    })
            .build();
    manager.start();
    try {
      Scheduler scheduler = new SchedulerImpl(zooKeeper);
      masterUpCounter.await();
      workerUpCounter.await();

      JobDefinition jd =
          JobDefinition.newBuilder(CloudTestJob.class)
              .jobData(TEST_NAME, name)
              .fixedRate(Duration.ofSeconds(5))
              .repeat(3)
              .build();
      scheduler.schedule(jd);
      assertThat(counter.await(15000L, TimeUnit.MILLISECONDS))
          .as("Job should run 3 times within 15 seconds")
          .isTrue();
    } finally {
      manager.shutdown();
    }
  }

  @Test
  @Timeout(25)
  public void testPauseResumeJob() throws Throwable {
    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch workerUpCounter = new CountDownLatch(1);
    AtomicReference<CountDownLatch> jobInCompleteCounter =
        new AtomicReference<>(new CountDownLatch(1));
    CountDownLatch jobInRemovedCounter = new CountDownLatch(3);
    CountDownLatch jobCompleteCounter = new CountDownLatch(1);

    String name = "testPauseResumeJob";
    counters.putIfAbsent(name, new CountDownLatch(3));

    CloudSchedulerManager manager =
        CloudSchedulerManager.newBuilder(zkUrl)
            .setThreadPool(threadPool)
            .setObserverSupplier(
                () ->
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
                      public void jobInstanceRemoved(UUID jobInId, UUID nodeId, Instant time) {
                        jobInRemovedCounter.countDown();
                      }

                      @Override
                      public void jobInstanceCompleted(
                          UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
                        logger.info("Job instance completed.");
                        jobInCompleteCounter.get().countDown();
                      }

                      @Override
                      public void jobDefinitionCompleted(UUID jobDefId, Instant time) {
                        jobCompleteCounter.countDown();
                      }
                    })
            .build();
    manager.start();
    try {
      Scheduler scheduler = new SchedulerImpl(zooKeeper);
      masterUpCounter.await();
      workerUpCounter.await();

      JobDefinition jd =
          JobDefinition.newBuilder(CloudTestJob.class)
              .jobData(TEST_NAME, name)
              .startAt(Instant.now().plusSeconds(1L))
              .fixedRate(Duration.ofSeconds(5))
              .repeat(3)
              .build();
      scheduler.schedule(jd);
      assertThat(jobInCompleteCounter.get().await(5L, TimeUnit.SECONDS))
          .as("Job should complete.")
          .isTrue();
      jobInCompleteCounter.set(new CountDownLatch(1));
      scheduler.pause(jd.getId(), true);
      assertThat(jobInCompleteCounter.get().await(6L, TimeUnit.SECONDS))
          .as("Job shouldn't run anymore.")
          .isFalse();
      scheduler.resume(jd.getId());
      assertThat(jobInCompleteCounter.get().await(6L, TimeUnit.SECONDS))
          .as("Job should complete again.")
          .isTrue();
      jobInCompleteCounter.set(new CountDownLatch(1));
      assertThat(jobInCompleteCounter.get().await(6L, TimeUnit.SECONDS))
          .as("Job should complete third time.")
          .isTrue();
      jobInCompleteCounter.set(new CountDownLatch(1));

      assertThat(jobInRemovedCounter.await(2L, TimeUnit.SECONDS))
          .as("Job instance should be removed.")
          .isTrue();
      assertThat(jobCompleteCounter.await(2L, TimeUnit.SECONDS))
          .as("Job definition should completed.")
          .isTrue();
    } finally {
      logger.info("Shutdown for pause/resume test.");
      manager.shutdown();
    }
  }

  public static class CloudTestJob implements Job {
    private static final Logger logger = LoggerFactory.getLogger(CloudTestJob.class);

    @Override
    public void execute(JobExecutionContext ctx) {
      logger.debug("Cloud test job got executed.");
      String key = (String) ctx.getExecutionData(TEST_NAME);
      CountDownLatch counter = counters.get(key);
      if (counter == null) {
        throw new IllegalStateException();
      }
      counter.countDown();
    }
  }
}
