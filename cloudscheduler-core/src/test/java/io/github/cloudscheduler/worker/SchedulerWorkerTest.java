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
import io.github.cloudscheduler.AsyncService;
import io.github.cloudscheduler.CloudSchedulerObserver;
import io.github.cloudscheduler.JobFactory;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.SimpleJobFactory;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Wei Gao */
public class SchedulerWorkerTest extends AbstractTest {
  private static final Logger logger = LoggerFactory.getLogger(SchedulerWorkerTest.class);
  private final JobFactory jobFactory = new SimpleJobFactory();

  private static final ConcurrentMap<String, CountDownLatch> counters = new ConcurrentHashMap<>();
  private static final String TEST_NAME = "name";

  @Test
  @Timeout(2)
  public void testBasicRunJobInstance() throws Throwable {
    String name = "testBasicRunJobInstance";
    CountDownLatch counter = counters.computeIfAbsent(name, (key) -> new CountDownLatch(1));
    SchedulerWorker worker =
        new SchedulerWorker(
            new Node(),
            zkUrl,
            Integer.MAX_VALUE,
            threadPool,
            jobFactory,
            new AbstractCloudSchedulerObserver() {});
    worker.start();
    try {
      JobDefinition jobDef =
          JobDefinition.newBuilder(LocalTestJob.class).jobData(TEST_NAME, name).build();
      jobService.saveJobDefinition(jobDef);
      jobService.scheduleJobInstance(jobDef);
      counter.await();
      Thread.sleep(100L);
      List<JobInstance> jis = jobService.getJobInstancesByJobDef(jobDef);

      assertThat(jis).hasSize(1);
      JobInstance ji = jis.iterator().next();
      assertThat(ji.getJobState().isComplete(false)).isTrue();
    } finally {
      worker.shutdown();
    }
  }

  @Test
  @Timeout(4)
  public void testTwoWorkerNode() throws Throwable {
    String name = "testTwoWorkerNode";
    CountDownLatch counter = counters.computeIfAbsent(name, (key) -> new CountDownLatch(1));
    Node node1 = new Node();
    final CountDownLatch jobInCompleteCouter = new CountDownLatch(1);
    CloudSchedulerObserver observer =
        new AbstractCloudSchedulerObserver() {
          @Override
          public void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
            jobInCompleteCouter.countDown();
          }
        };
    SchedulerWorker worker1 =
        new SchedulerWorker(node1, zkUrl, Integer.MAX_VALUE, threadPool, jobFactory, observer);
    Node node2 = new Node();
    SchedulerWorker worker2 =
        new SchedulerWorker(node2, zkUrl, Integer.MAX_VALUE, threadPool, jobFactory, observer);

    worker1.start();
    worker2.start();
    try {
      JobDefinition jobDef =
          JobDefinition.newBuilder(LocalTestJob.class).jobData(TEST_NAME, name).build();
      jobService.saveJobDefinition(jobDef);
      jobService.scheduleJobInstance(jobDef);
      counter.await();
      jobInCompleteCouter.await();
      List<JobInstance> jis = jobService.getJobInstancesByJobDef(jobDef);
      assertThat(jis).hasSize(1);
      JobInstance ji = jis.iterator().next();
      assertThat(ji.getJobState().isComplete(false)).isTrue();

      JobDefinitionStatus status = jobService.getJobStatusById(jobDef.getId());

      assertThat(status.getRunCount()).isEqualTo(1);

      jobService.deleteJobInstance(ji.getId());
      Thread.sleep(100L);

      JobInstanceProcessor processor = worker1.getJobProcessorById(ji.getId());
      assertThat(processor).isNull();
      processor = worker2.getJobProcessorById(ji.getId());
      assertThat(processor).isNull();

      logger.trace("Test done, shutdown two worker nodes.");
    } finally {
      worker2.shutdown();
      worker1.shutdown();
    }
  }

  @Test
  @Timeout(25)
  public void testMultipleWorkerNodeGlobalJob() throws Throwable {
    int numberOfWorkers = 50;
    String name = "testMultipleWorkerNodeGlobalJob";
    CountDownLatch counter =
        counters.computeIfAbsent(name, (key) -> new CountDownLatch(numberOfWorkers));
    Map<Node, SchedulerWorker> workers = new HashMap<>();
    final CountDownLatch workerUpCounter = new CountDownLatch(numberOfWorkers);
    final CountDownLatch jobInCompleteCounter = new CountDownLatch(numberOfWorkers);
    for (int i = 0; i < numberOfWorkers; i++) {
      Node node = new Node();
      SchedulerWorker worker =
          new SchedulerWorker(
              node,
              zkUrl,
              Integer.MAX_VALUE,
              threadPool,
              jobFactory,
              new AbstractCloudSchedulerObserver() {
                @Override
                public void workerNodeUp(UUID nodeId, Instant time) {
                  workerUpCounter.countDown();
                }

                @Override
                public void jobInstanceCompleted(
                    UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
                  jobInCompleteCounter.countDown();
                }
              });
      workers.put(node, worker);
      worker.start();
    }
    // Wait till all worker up
    workerUpCounter.await();

    try {
      JobDefinition jobDef =
          JobDefinition.newBuilder(LocalTestJob.class).jobData(TEST_NAME, name).global().build();
      jobService.saveJobDefinition(jobDef);
      logger.trace("Global job saved.");
      JobInstance instance = jobService.scheduleJobInstance(jobDef);
      logger.info("JobInstance: {} scheduled", instance.getId());
      counter.await();
      jobInCompleteCounter.await();
      logger.info("Checking JobInstance status");
      List<JobInstance> jis = jobService.getJobInstancesByJobDef(jobDef);
      assertThat(jis).hasSize(1);
      JobInstance ji = jis.iterator().next();
      assertThat(ji.getJobState().isComplete(true)).isTrue();

      JobDefinitionStatus status = jobService.getJobStatusById(jobDef.getId());

      assertThat(status.getRunCount()).isEqualTo(1);

      jobService.deleteJobInstance(ji.getId());
      Thread.sleep(100L);

      workers
          .values()
          .forEach(
              worker -> {
                JobInstanceProcessor processor = worker.getJobProcessorById(ji.getId());
                assertThat(processor).isNull();
              });
    } finally {
      workers.values().forEach(AsyncService::shutdown);
    }
  }

  static void countDown(String key) {
    CountDownLatch counter = counters.get(key);
    if (counter == null) {
      throw new IllegalStateException();
    }
    counter.countDown();
  }
}
