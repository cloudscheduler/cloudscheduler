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
import io.github.cloudscheduler.JobFactory;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.SimpleJobFactory;
import io.github.cloudscheduler.TestJob;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionState;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.service.JobService;
import io.github.cloudscheduler.service.JobServiceImpl;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Wei Gao */
public class SchedulerMasterConnectTest {
  private static final Logger logger = LoggerFactory.getLogger(SchedulerMasterConnectTest.class);
  private final JobFactory jobFactory = new SimpleJobFactory();
  private static ExecutorService threadPool;

  @BeforeAll
  public static void setup() {
    AtomicInteger threadCounter = new AtomicInteger(0);
    threadPool =
        Executors.newCachedThreadPool(
            r -> new Thread(r, "TestThread-" + threadCounter.incrementAndGet()));
  }

  @AfterAll
  public static void teardown() {
    if (threadPool != null) {
      threadPool.shutdown();
    }
  }

  @Test
  @Timeout(10)
  public void testMasterKeepRetry() throws Throwable {
    int port = AbstractTest.randomPort();
    logger.info("Creating a zookeeper on port {}", port);
    final CountDownLatch masterUpCounter = new CountDownLatch(1);
    final CountDownLatch masterDownCounter = new CountDownLatch(1);
    final CountDownLatch jobDefFinishedCounter = new CountDownLatch(1);
    TestingServer zkTestServer = new TestingServer(port);
    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkTestServer.getConnectString(),
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void masterNodeUp(UUID nodeId, Instant time) {
                masterUpCounter.countDown();
              }

              @Override
              public void masterNodeDown(UUID nodeId, Instant time) {
                masterDownCounter.countDown();
              }

              @Override
              public void jobDefinitionCompleted(UUID id, Instant time) {
                jobDefFinishedCounter.countDown();
              }
            });
    logger.info("Starting scheduler master");
    master.start();
    try {
      masterUpCounter.await();
      logger.info("Close zookeeper on port {}", port);
      zkTestServer.close();
      masterDownCounter.await();
      logger.info("Recreate a zookeeper on port {}", port);
      zkTestServer = new TestingServer(port);
      ZooKeeper zooKeeper =
          ZooKeeperUtils.connectToZooKeeper(zkTestServer.getConnectString(), Integer.MAX_VALUE)
              .get();
      try {
        JobService jobService = new JobServiceImpl(zooKeeper);
        JobDefinition job = JobDefinition.newBuilder(TestJob.class).build();
        jobService.saveJobDefinition(job);
        jobDefFinishedCounter.await();
        JobDefinitionStatus status = jobService.getJobStatusById(job.getId());
        assertThat(status.getState()).isEqualTo(JobDefinitionState.FINISHED);
      } finally {
        master.shutdown();
        zooKeeper.close();
      }
    } finally {
      logger.info("Shutting down master.");
      zkTestServer.close();
    }
  }

  @Test
  @Timeout(5)
  public void testShutdownMasterWhileZKDown() throws Throwable {
    int port = AbstractTest.randomPort();
    logger.info("Creating a zookeeper on port {}", port);
    final CountDownLatch masterUpCounter = new CountDownLatch(1);
    final CountDownLatch masterDownCounter = new CountDownLatch(1);
    TestingServer zkTestServer = new TestingServer(port);
    SchedulerMaster master =
        new SchedulerMaster(
            new Node(),
            zkTestServer.getConnectString(),
            Integer.MAX_VALUE,
            jobFactory,
            threadPool,
            new AbstractCloudSchedulerObserver() {
              @Override
              public void masterNodeUp(UUID nodeId, Instant time) {
                masterUpCounter.countDown();
              }

              @Override
              public void masterNodeDown(UUID nodeId, Instant time) {
                masterDownCounter.countDown();
              }
            });
    logger.info("Starting scheduler master");
    master.start();
    try {
      masterUpCounter.await();
      logger.info("Close zookeeper on port {}", port);
      zkTestServer.close();
      masterDownCounter.await();
      master.shutdown();
    } finally {
      zkTestServer.close();
    }
  }
}
