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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Wei Gao
 */
public class CloudSchedulerManagerTest extends AbstractTest {
  private static final Logger logger = LoggerFactory.getLogger(CloudSchedulerManagerTest.class);

  private static final ConcurrentMap<String, CountDownLatch> counters = new ConcurrentHashMap<>();
  private static final String TEST_NAME = "name";

  @Test(timeOut = 3000L)
  public void testStartMasterAndWorker() throws Throwable {
    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch workerUpCounter = new CountDownLatch(1);
    CloudSchedulerManager manager = CloudSchedulerManager
        .newBuilder(zkUrl)
        .setThreadPool(threadPool)
        .setObserverSupplier(() -> new AbstractCloudSchedulerObserver() {
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
      Assert.assertNotNull(workers);
      Assert.assertEquals(workers.size(), 1);

      logger.trace("ZooKeeper: {}", zooKeeper);
      List<String> masters = ZooKeeperUtils.getChildren(zooKeeper, "/lock/master").get();
      Assert.assertEquals(masters.size(), 1);
    } finally {
      manager.shutdown();
    }
  }

  @Test(timeOut = 3000L)
  public void testSingleJob() throws Throwable {
    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch workerUpCounter = new CountDownLatch(1);
    String name = "testSingleJob";
    counters.putIfAbsent(name, new CountDownLatch(1));
    CountDownLatch counter = counters.get(name);
    CloudSchedulerManager manager = CloudSchedulerManager
        .newBuilder(zkUrl)
        .setThreadPool(threadPool)
        .setObserverSupplier(() -> new AbstractCloudSchedulerObserver() {
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

      JobDefinition jd = JobDefinition.newBuilder(CloudTestJob.class)
          .jobData(TEST_NAME, name)
          .runNow()
          .build();
      scheduler.schedule(jd);
      Assert.assertTrue(counter.await(1000L, TimeUnit.MILLISECONDS), "Job run be run.");
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
    CloudSchedulerManager manager = CloudSchedulerManager
        .newBuilder(zkUrl)
        .setThreadPool(threadPool)
        .setObserverSupplier(() -> new AbstractCloudSchedulerObserver() {
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

      JobDefinition jd = JobDefinition.newBuilder(CloudTestJob.class)
          .jobData(TEST_NAME, name)
          .fixedRate(Duration.ofSeconds(5))
          .repeat(3).build();
      scheduler.schedule(jd);
      Assert.assertTrue(counter.await(15000L, TimeUnit.MILLISECONDS), "Job should run 3 times within 15 seconds");
    } finally {
      manager.shutdown();
    }
  }

  @Test(timeOut = 20000L)
  public void testPauseResumeJob() throws Throwable {
    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch workerUpCounter = new CountDownLatch(1);
    AtomicReference<CountDownLatch> jobInCompleteCounter = new AtomicReference<>(new CountDownLatch(1));
    CountDownLatch jobInRemovedCounter = new CountDownLatch(3);
    CountDownLatch jobCompleteCounter = new CountDownLatch(1);

    String name = "testPauseResumeJob";
    counters.putIfAbsent(name, new CountDownLatch(3));

    CloudSchedulerManager manager = CloudSchedulerManager
        .newBuilder(zkUrl)
        .setThreadPool(threadPool)
        .setObserverSupplier(() -> new AbstractCloudSchedulerObserver() {
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
          public void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
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

      JobDefinition jd = JobDefinition.newBuilder(CloudTestJob.class)
          .jobData(TEST_NAME, name)
          .startAt(Instant.now().plusSeconds(1L))
          .fixedRate(Duration.ofSeconds(5))
          .repeat(3).build();
      scheduler.schedule(jd);
      Assert.assertTrue(jobInCompleteCounter.get().await(5L, TimeUnit.SECONDS), "Job should complete.");
      jobInCompleteCounter.set(new CountDownLatch(1));
      scheduler.pause(jd.getId(), true);
      Assert.assertFalse(jobInCompleteCounter.get().await(6L, TimeUnit.SECONDS), "Job shouldn't run anymore.");
      scheduler.resume(jd.getId());
      Assert.assertTrue(jobInCompleteCounter.get().await(6L, TimeUnit.SECONDS), "Job should complete again.");
      jobInCompleteCounter.set(new CountDownLatch(1));
      Assert.assertTrue(jobInCompleteCounter.get().await(6L, TimeUnit.SECONDS), "Job should complete third time.");
      jobInCompleteCounter.set(new CountDownLatch(1));

      Assert.assertTrue(jobInRemovedCounter.await(2L, TimeUnit.SECONDS), "Job instance should be removed.");
      Assert.assertTrue(jobCompleteCounter.await(2L, TimeUnit.SECONDS), "Job definition should completed.");
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
