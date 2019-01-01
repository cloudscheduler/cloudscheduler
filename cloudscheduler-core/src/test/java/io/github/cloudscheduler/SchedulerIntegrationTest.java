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

import io.github.cloudscheduler.master.SchedulerMaster;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionState;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import io.github.cloudscheduler.worker.SchedulerWorker;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Wei Gao
 */
public class SchedulerIntegrationTest extends AbstractTest {

  @Test(timeOut = 6000L)
  public void testScheduleJobWithOneMasterOneWorker() throws Throwable {
    final CountDownLatch jobInStartedCounter = new CountDownLatch(1);
    final CountDownLatch jobInCompleteCounter = new CountDownLatch(1);
    final CountDownLatch jobInRemovedCounter = new CountDownLatch(1);
    CloudSchedulerObserver observer = new AbstractCloudSchedulerObserver() {
      @Override
      public void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
        jobInCompleteCounter.countDown();
      }

      @Override
      public void jobInstanceStarted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
        jobInStartedCounter.countDown();
      }

      @Override
      public void jobInstanceRemoved(UUID jobDefId, UUID jobInId, Instant time) {
        jobInRemovedCounter.countDown();
      }
    };

    SchedulerWorker worker = new SchedulerWorker(new Node(), zkUrl,
        Integer.MAX_VALUE, threadPool, new SimpleJobFactory(), observer);
    SchedulerMaster master = new SchedulerMaster(new Node(), zkUrl, Integer.MAX_VALUE, observer);
    worker.start();
    master.start();

    try {
      Instant t = Instant.now().plus(Duration.ofSeconds(1));
      JobDefinition job = JobDefinition.newBuilder(TestSleepJob.class)
          .startAt(t).build();
      jobService.saveJobDefinition(job);
      JobDefinitionStatus status = jobService.getJobStatusById(job.getId());
      Assert.assertEquals(status.getState(), JobDefinitionState.CREATED);

      Assert.assertNull(status.getLastScheduleTime());

      Assert.assertTrue(jobInStartedCounter.await(1500L, TimeUnit.MILLISECONDS), "Job instance should started.");

      List<JobInstance> jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 1);
      JobInstance ji = jis.iterator().next();
      Assert.assertFalse(ji.getJobState().isComplete(false));

      Assert.assertFalse(jobInCompleteCounter.await(2L, TimeUnit.SECONDS), "Job shouldn't complete yet.");
      Assert.assertTrue(jobInCompleteCounter.await(2L, TimeUnit.SECONDS), "Job should complete");
      jobInRemovedCounter.await();
      jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 0);

      status = jobService.getJobStatusById(job.getId());
      Assert.assertEquals(status.getState(), JobDefinitionState.FINISHED);
    } finally {
      master.shutdown();
      worker.shutdown();
    }
  }

  @Test(timeOut = 10000L)
  public void testScheduleJobWithMasterFailed() throws Throwable {
    final AtomicReference<CountDownLatch> masterUpCounter = new AtomicReference<>(new CountDownLatch(1));
    final CountDownLatch masterDownCounter = new CountDownLatch(1);
    final CountDownLatch jobInScheduledCounter = new CountDownLatch(1);
    final CountDownLatch jobInCompleteCounter = new CountDownLatch(1);
    final CountDownLatch jobInRemovedCounter = new CountDownLatch(1);
    CloudSchedulerObserver observer = new AbstractCloudSchedulerObserver() {
      @Override
      public void masterNodeUp(UUID nodeId, Instant time) {
        masterUpCounter.get().countDown();
      }

      @Override
      public void masterNodeDown(UUID nodeId, Instant time) {
        masterDownCounter.countDown();
      }

      @Override
      public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
        jobInScheduledCounter.countDown();
      }

      @Override
      public void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
        jobInCompleteCounter.countDown();
      }

      @Override
      public void jobInstanceRemoved(UUID jobDefId, UUID jobInId, Instant time) {
        jobInRemovedCounter.countDown();
      }
    };
    SchedulerWorker worker = new SchedulerWorker(new Node(), zkUrl,
        Integer.MAX_VALUE, threadPool, new SimpleJobFactory(), observer);
    SchedulerMaster master1 = new SchedulerMaster(new Node(), zkUrl, Integer.MAX_VALUE, observer);
    SchedulerMaster master2 = new SchedulerMaster(new Node(), zkUrl, Integer.MAX_VALUE, observer);
    worker.start();
    // Make sure master is leader
    master1.start();
    masterUpCounter.get().await();
    masterUpCounter.set(new CountDownLatch(1));
    master2.start();
    try {
      /* Create Job with 2 seconds delay */
      Instant t = Instant.now().plus(Duration.ofSeconds(2));
      JobDefinition job = JobDefinition.newBuilder(TestSleepJob.class)
          .startAt(t).build();
      jobService.saveJobDefinition(job);
      JobDefinitionStatus status = jobService.getJobStatusById(job.getId());
      Assert.assertEquals(status.getState(), JobDefinitionState.CREATED);

      /* shutdown one master , another master take over */
      master1.shutdown();

      /* no job instance created yet*/
      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(is);
      Assert.assertTrue(is.isEmpty());
      Assert.assertNull(status.getLastScheduleTime());

      Assert.assertFalse(jobInScheduledCounter.await(1L, TimeUnit.SECONDS), "Job instance shouldn't be scheduled yet.");
      Assert.assertTrue(jobInScheduledCounter.await(2L, TimeUnit.SECONDS), "Job instance should be scheduled yet.");
      Assert.assertFalse(jobInCompleteCounter.await(1L, TimeUnit.SECONDS), "Job instance shouldn't complete yet.");

      /* new master scheduled job instance */
      List<JobInstance> jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 1);
      JobInstance ji = jis.iterator().next();
      Assert.assertFalse(ji.getJobState().isComplete(false));

      Assert.assertTrue(jobInCompleteCounter.await(4L, TimeUnit.SECONDS), "Job instance should complete.");

      jobInRemovedCounter.await();

      /* job finished and the job instance has been removed by master */
      jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 0);

      status = jobService.getJobStatusById(job.getId());
      Assert.assertEquals(status.getState(), JobDefinitionState.FINISHED);

      Assert.assertNotNull(status.getLastScheduleTime());
    } finally {
      master2.shutdown();
      worker.shutdown();
    }
  }

  @Test(timeOut = 10000L)
  public void testScheduleJobWithMasterFailedAndThenStartNewMaster() throws Throwable {
    final AtomicReference<CountDownLatch> masterUpCounter = new AtomicReference<>(new CountDownLatch(1));
    final CountDownLatch jobInScheduledCounter = new CountDownLatch(1);
    final CountDownLatch jobInCompleteCounter = new CountDownLatch(1);
    final CountDownLatch jobInRemovedCounter = new CountDownLatch(1);
    CloudSchedulerObserver observer = new AbstractCloudSchedulerObserver() {
      @Override
      public void masterNodeUp(UUID nodeId, Instant time) {
        masterUpCounter.get().countDown();
      }

      @Override
      public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
        jobInScheduledCounter.countDown();
      }

      @Override
      public void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
        jobInCompleteCounter.countDown();
      }

      @Override
      public void jobInstanceRemoved(UUID jobDefId, UUID jobInId, Instant time) {
        jobInRemovedCounter.countDown();
      }
    };
    SchedulerWorker worker = new SchedulerWorker(new Node(), zkUrl,
        Integer.MAX_VALUE, threadPool, new SimpleJobFactory(), observer);
    SchedulerMaster master1 = new SchedulerMaster(new Node(), zkUrl, Integer.MAX_VALUE, observer);
    SchedulerMaster master2 = new SchedulerMaster(new Node(), zkUrl, Integer.MAX_VALUE, observer);
    worker.start();
    master1.start();

    try {
      masterUpCounter.get().await();
      masterUpCounter.set(new CountDownLatch(1));

      /* Create Job with 2 seconds delay */
      Instant t = Instant.now().plus(Duration.ofSeconds(2));
      JobDefinition job = JobDefinition.newBuilder(TestSleepJob.class)
          .startAt(t).build();
      jobService.saveJobDefinition(job);
      JobDefinitionStatus status = jobService.getJobStatusById(job.getId());
      Assert.assertEquals(status.getState(), JobDefinitionState.CREATED);

      /* shutdown master */
      master1.shutdown();

      /* no job instance created */
      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(is);
      Assert.assertTrue(is.isEmpty());
      Assert.assertNull(status.getLastScheduleTime());

      /* start new master */
      master2.start();
      Assert.assertTrue(jobInScheduledCounter.await(3500L, TimeUnit.MILLISECONDS), "Job instance should be scheduled");

      /* one job instance created and not finished yet */
      List<JobInstance> jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 1);
      JobInstance ji = jis.iterator().next();
      Assert.assertFalse(ji.getJobState().isComplete(false));

      Assert.assertTrue(jobInCompleteCounter.await(5L, TimeUnit.SECONDS), "Job instance should completed");
      jobInRemovedCounter.await();

      /* job finished and job instance has been removed by master */
      jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 0);

      status = jobService.getJobStatusById(job.getId());
      Assert.assertEquals(status.getState(), JobDefinitionState.FINISHED);

      Assert.assertNotNull(status.getLastScheduleTime());
    } finally {
      master2.shutdown();
      worker.shutdown();
    }
  }

  @Test(timeOut = 10000L)
  public void testScheduleJobWithWorkerFailed() throws Throwable {
    Node workerNode = new Node();
    CountDownLatch jobInStartCounter = new CountDownLatch(1);
    CountDownLatch jobInCompletedCounter = new CountDownLatch(1);
    CountDownLatch jobInRemovedCounter = new CountDownLatch(1);
    CloudSchedulerObserver observer = new AbstractCloudSchedulerObserver() {
      @Override
      public void jobInstanceStarted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
        jobInStartCounter.countDown();
      }

      @Override
      public void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
        jobInCompletedCounter.countDown();
      }

      @Override
      public void jobInstanceRemoved(UUID jobDefId, UUID jobInId, Instant time) {
        jobInRemovedCounter.countDown();
      }
    };
    SchedulerWorker worker1 = new SchedulerWorker(workerNode, zkUrl,
        Integer.MAX_VALUE, threadPool, new SimpleJobFactory(), observer);
    SchedulerMaster master = new SchedulerMaster(workerNode, zkUrl, Integer.MAX_VALUE, observer);

    worker1.start();
    master.start();
    SchedulerWorker worker2 = new SchedulerWorker(workerNode, zkUrl,
        Integer.MAX_VALUE, threadPool, new SimpleJobFactory(), observer);

    try {
      /* Create Job with 1 second delay */
      Instant t = Instant.now().plus(Duration.ofSeconds(1));
      JobDefinition job = JobDefinition.newBuilder(TestSleepJob.class)
          .startAt(t).build();
      jobService.saveJobDefinition(job);
      JobDefinitionStatus status = jobService.getJobStatusById(job.getId());
      Assert.assertEquals(status.getState(), JobDefinitionState.CREATED);

      /* master did not schedule any job yet */
      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(is);
      Assert.assertTrue(is.isEmpty());
      Assert.assertNull(status.getLastScheduleTime());

      Assert.assertTrue(jobInStartCounter.await(2L, TimeUnit.SECONDS), "Job instance should started");

      /* master create one job instance , worker picked it up , marked as running */
      List<JobInstance> jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 1);
      JobInstance ji = jis.iterator().next();
      Assert.assertEquals(ji.getRunStatus().get(workerNode.getId()).getState(), JobInstanceState.RUNNING);
      Assert.assertFalse(ji.getJobState().isComplete(false));

      /* shutdown worker */
      worker1.shutdown();
      Assert.assertFalse(jobInCompletedCounter.await(2L, TimeUnit.SECONDS), "Job instance shouldn't complete");

      /* job instance still exists , not finished yet */
      jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 1);
      ji = jis.iterator().next();
      Assert.assertFalse(ji.getJobState().isComplete(false));

      /* start new worker */
      worker2.start();

      Assert.assertTrue(jobInCompletedCounter.await(4L, TimeUnit.SECONDS), "Job instance should complete");
      jobInRemovedCounter.await();

      /* new worker finished job , master removed job instance */
      jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 0);

      status = jobService.getJobStatusById(job.getId());
      Assert.assertEquals(status.getState(), JobDefinitionState.FINISHED);
      Assert.assertTrue(status.getJobInstanceState().isEmpty());
    } finally {
      worker2.shutdown();
      master.shutdown();
    }
  }

  @Test(timeOut = 9000L)
  public void testScheduleGlobalJobWithWorkerFailed() throws Throwable {
    Node workerNode1 = new Node();
    Node workerNode2 = new Node();
    Node workerNode3 = new Node();
    CountDownLatch jobInStartedCounter = new CountDownLatch(3);
    CountDownLatch workerDownCounter = new CountDownLatch(1);
    CountDownLatch jobInCompletedCounter = new CountDownLatch(2);
    CountDownLatch jobInRemovedCounter = new CountDownLatch(1);
    CountDownLatch workerRemovedCounter = new CountDownLatch(1);
    CloudSchedulerObserver observer = new AbstractCloudSchedulerObserver() {
      @Override
      public void workerNodeDown(UUID nodeId, Instant time) {
        workerDownCounter.countDown();
      }

      @Override
      public void workerNodeRemoved(UUID nodeId, Instant time) {
        workerRemovedCounter.countDown();
      }

      @Override
      public void jobInstanceStarted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
        jobInStartedCounter.countDown();
      }

      @Override
      public void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
        jobInCompletedCounter.countDown();
      }

      @Override
      public void jobInstanceRemoved(UUID jobDefId, UUID jobInId, Instant time) {
        jobInRemovedCounter.countDown();
      }
    };
    SchedulerWorker worker1 = new SchedulerWorker(workerNode1, zkUrl,
        Integer.MAX_VALUE, threadPool, new SimpleJobFactory(), observer);
    SchedulerMaster master = new SchedulerMaster(new Node(), zkUrl, Integer.MAX_VALUE, observer);
    SchedulerWorker worker2 = new SchedulerWorker(workerNode2, zkUrl,
        Integer.MAX_VALUE, threadPool, new SimpleJobFactory(), observer);
    SchedulerWorker worker3 = new SchedulerWorker(workerNode3, zkUrl,
        Integer.MAX_VALUE, threadPool, new SimpleJobFactory(), observer);

    worker1.start();
    worker2.start();
    worker3.start();
    master.start();

    try {
      Instant t = Instant.now().plus(Duration.ofSeconds(1));
      JobDefinition job = JobDefinition.newBuilder(TestSleepJob.class)
          .startAt(t).global().build();

      jobService.saveJobDefinition(job);
      JobDefinitionStatus status = jobService.getJobStatusById(job.getId());
      Assert.assertEquals(status.getState(), JobDefinitionState.CREATED);

      /* master did not schedule any job yet */
      List<JobInstance> is = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(is);
      Assert.assertTrue(is.isEmpty());
      Assert.assertNull(status.getLastScheduleTime());

      Assert.assertTrue(jobInStartedCounter.await(2L, TimeUnit.SECONDS), "Job instance should started");

      /* master create one job instance , worker picked it up , marked as running */
      List<JobInstance> jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 1);
      JobInstance ji = jis.iterator().next();
      Assert.assertEquals(ji.getRunStatus().get(workerNode1.getId()).getState(), JobInstanceState.RUNNING);
      Assert.assertEquals(ji.getRunStatus().get(workerNode2.getId()).getState(), JobInstanceState.RUNNING);
      Assert.assertEquals(ji.getRunStatus().get(workerNode3.getId()).getState(), JobInstanceState.RUNNING);
      Assert.assertFalse(ji.getJobState().isComplete(false));

      /* shutdown worker */
      worker1.shutdown();
      workerDownCounter.await();
      workerRemovedCounter.await();

      jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 1);
      ji = jis.iterator().next();
      Assert.assertEquals(ji.getRunStatus().get(workerNode1.getId()).getState(), JobInstanceState.NODE_FAILED);

      Assert.assertTrue(jobInCompletedCounter.await(4L, TimeUnit.SECONDS), "Job instance should completed");
      jobInRemovedCounter.await();
      /* rest worker finished job , master removed job instance */
      jis = jobService.getJobInstancesByJobDef(job);
      Assert.assertNotNull(jis);
      Assert.assertEquals(jis.size(), 0);

      status = jobService.getJobStatusById(job.getId());
      Assert.assertEquals(status.getState(), JobDefinitionState.FINISHED);
      Assert.assertTrue(status.getJobInstanceState().isEmpty());
    } finally {
      worker2.shutdown();
      worker3.shutdown();
      master.shutdown();
    }
  }

  @Test
  public void testGlobalJobCompleteWithoutWorker() throws Throwable {
    CountDownLatch masterUpCounter = new CountDownLatch(1);
    CountDownLatch jobDefCompleteCounter = new CountDownLatch(1);
    SchedulerMaster master = new SchedulerMaster(new Node(), zkUrl, Integer.MAX_VALUE,
        new AbstractCloudSchedulerObserver() {
          @Override
          public void masterNodeUp(UUID id, Instant time) {
            masterUpCounter.countDown();
          }

          @Override
          public void jobDefinitionCompleted(UUID id, Instant time) {
            jobDefCompleteCounter.countDown();
          }
        });
    master.start();
    try {
      masterUpCounter.await();
      JobDefinition job = JobDefinition.newBuilder(TestSleepJob.class)
          .global()
          .build();
      jobService.saveJobDefinition(job);
      Assert.assertTrue(jobDefCompleteCounter.await(1L, TimeUnit.SECONDS), "Job definition should completed.");
    } finally {
      master.shutdown();
    }
  }

  @Test
  public void testWorkerStartAfterJobInstanceCreated() throws Throwable {
    CountDownLatch jobInstanceScheduledCounter = new CountDownLatch(1);
    CountDownLatch jobInstanceCompletedCounter = new CountDownLatch(1);
    CountDownLatch jobInstanceRemovedCounter = new CountDownLatch(1);
    CountDownLatch jobDefCompletedCounter = new CountDownLatch(1);

    CloudSchedulerObserver observer = new AbstractCloudSchedulerObserver() {
      @Override
      public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
        jobInstanceScheduledCounter.countDown();
      }

      @Override
      public void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
        jobInstanceCompletedCounter.countDown();
      }

      @Override
      public void jobInstanceRemoved(UUID jobDefId, UUID jobInId, Instant time) {
        jobInstanceRemovedCounter.countDown();
      }

      @Override
      public void jobDefinitionCompleted(UUID id, Instant time) {
        jobDefCompletedCounter.countDown();
      }
    };
    SchedulerMaster master = new SchedulerMaster(new Node(), zkUrl, Integer.MAX_VALUE, observer);
    SchedulerWorker worker = new SchedulerWorker(new Node(), zkUrl,
        Integer.MAX_VALUE, threadPool, new SimpleJobFactory(), observer);
    master.start();
    try {
      JobDefinition job = JobDefinition.newBuilder(TestSleepJob.class).build();
      jobService.saveJobDefinition(job);

      Assert.assertTrue(jobInstanceScheduledCounter.await(1L, TimeUnit.SECONDS), "Job instance should been scheduled.");
      Assert.assertFalse(jobInstanceCompletedCounter.await(4L, TimeUnit.SECONDS), "Job instance shouldn't complete.");

      worker.start();
      Assert.assertTrue(jobInstanceCompletedCounter.await(4L, TimeUnit.SECONDS), "Job instance should complete.");
      Assert.assertTrue(jobInstanceRemovedCounter.await(1L, TimeUnit.SECONDS), "Job instance should been removed");
      Assert.assertTrue(jobDefCompletedCounter.await(1L, TimeUnit.SECONDS), "Job definition should complete");
    } finally {
      worker.shutdown();
      master.shutdown();
    }
  }
}
