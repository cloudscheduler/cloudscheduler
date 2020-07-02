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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cloudscheduler.AbstractCloudSchedulerObserver;
import io.github.cloudscheduler.AbstractTest;
import io.github.cloudscheduler.JobFactory;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.SimpleJobFactory;
import io.github.cloudscheduler.TestJob;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import io.github.cloudscheduler.service.JobService;
import io.github.cloudscheduler.service.JobServiceImpl;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Wei Gao */
public class SchedulerWorkerConnectTest {
  private static final Logger logger = LoggerFactory.getLogger(SchedulerWorkerConnectTest.class);
  private final JobFactory jobFactory = new SimpleJobFactory();

  @Test
  @Timeout(10)
  public void testWorkerKeepRetry() throws Throwable {
    int port = AbstractTest.randomPort();
    logger.info("Creating a zookeeper on port {}", port);
    TestingServer zkTestServer = new TestingServer(port);
    ExecutorService threadPool = Executors.newCachedThreadPool();

    final CountDownLatch workerUpCounter = new CountDownLatch(1);
    final CountDownLatch workerDownCounter = new CountDownLatch(1);
    final CountDownLatch jobInCompleteCounter = new CountDownLatch(1);

    SchedulerWorker worker =
        new SchedulerWorker(
            new Node(),
            zkTestServer.getConnectString(),
            Integer.MAX_VALUE,
            threadPool,
            jobFactory,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void workerNodeUp(UUID nodeId, Instant time) {
                workerUpCounter.countDown();
              }

              @Override
              public void workerNodeDown(UUID nodeId, Instant time) {
                workerDownCounter.countDown();
              }

              @Override
              public void jobInstanceCompleted(
                  UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
                jobInCompleteCounter.countDown();
              }
            });
    logger.info("Starting scheduler worker");
    worker.start();
    try {
      workerUpCounter.await();
      logger.info("Close zookeeper on port {}", port);
      zkTestServer.close();
      workerDownCounter.await();
      logger.info("Recreate a zookeeper on port {}", port);
      zkTestServer = new TestingServer(port);
      ZooKeeper zooKeeper =
          ZooKeeperUtils.connectToZooKeeper(zkTestServer.getConnectString(), Integer.MAX_VALUE)
              .get();
      try {
        JobService jobService = new JobServiceImpl(zooKeeper);
        Instant start = Instant.now().plusSeconds(1);
        JobDefinition job = JobDefinition.newBuilder(TestJob.class).startAt(start).build();
        jobService.saveJobDefinition(job);
        JobInstance jobIn = jobService.scheduleJobInstance(job);
        jobIn = jobService.getJobInstanceById(jobIn.getId());
        assertThat(jobIn).isNotNull();
        assertThat(jobIn.getJobState()).isEqualTo(JobInstanceState.SCHEDULED);
        assertThat(jobInCompleteCounter.await(2500L, TimeUnit.MILLISECONDS)).isTrue();
        jobIn = jobService.getJobInstanceById(jobIn.getId());
        assertThat(jobIn);
        assertThat(jobIn.getJobState()).isEqualTo(JobInstanceState.COMPLETE);
      } finally {
        worker.shutdown();
        zooKeeper.close();
      }
    } finally {
      zkTestServer.close();
      threadPool.shutdown();
    }
  }

  @Test
  @Timeout(5)
  public void testShutdownWorkerWhileZKDown() throws Throwable {
    int port = AbstractTest.randomPort();
    logger.info("Creating a zookeeper on port {}", port);
    TestingServer zkTestServer = new TestingServer(port);
    ExecutorService threadPool = Executors.newCachedThreadPool();
    CountDownLatch workerUpCounter = new CountDownLatch(1);
    CountDownLatch workerDownCounter = new CountDownLatch(1);
    SchedulerWorker worker =
        new SchedulerWorker(
            new Node(),
            zkTestServer.getConnectString(),
            Integer.MAX_VALUE,
            threadPool,
            jobFactory,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void workerNodeUp(UUID nodeId, Instant time) {
                workerUpCounter.countDown();
              }

              @Override
              public void workerNodeDown(UUID nodeId, Instant time) {
                workerDownCounter.countDown();
              }
            });
    logger.info("Starting scheduler master");
    worker.start();
    try {
      workerUpCounter.await();
      logger.info("Close zookeeper on port {}", port);
      zkTestServer.close();
      workerDownCounter.await();
      worker.shutdown();
    } finally {
      zkTestServer.close();
      threadPool.shutdown();
    }
  }
}
