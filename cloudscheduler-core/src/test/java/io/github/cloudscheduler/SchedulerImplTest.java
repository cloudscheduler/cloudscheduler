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
import io.github.cloudscheduler.model.JobDefinitionState;
import io.github.cloudscheduler.model.JobDefinitionStatus;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SchedulerImplTest extends AbstractTest {
  @Test
  public void testScheduleRunOnce() {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    JobDefinition jobDef = scheduler.runNow(TestJob.class);
    Assert.assertNotNull(jobDef);
    Assert.assertEquals(jobDef.getMode(), JobDefinition.ScheduleMode.START_NOW);
    Assert.assertFalse(jobDef.isGlobal());
    Assert.assertNull(jobDef.getCron());
    Assert.assertNull(jobDef.getEndTime());
    Assert.assertNull(jobDef.getStartTime());
    Assert.assertNull(jobDef.getDelay());
    Assert.assertNull(jobDef.getRate());
    Assert.assertNull(jobDef.getRepeat());
  }

  @Test
  public void testScheduleRunNow() {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = scheduler.runOnce(TestJob.class, time);
    Assert.assertNotNull(jobDef);
    Assert.assertEquals(jobDef.getMode(), JobDefinition.ScheduleMode.START_AT);
    Assert.assertFalse(jobDef.isGlobal());
    Assert.assertNull(jobDef.getCron());
    Assert.assertNull(jobDef.getEndTime());
    Assert.assertEquals(jobDef.getStartTime(), time);
    Assert.assertNull(jobDef.getDelay());
    Assert.assertNull(jobDef.getRate());
    Assert.assertNull(jobDef.getRepeat());
  }

  @Test
  public void testScheduleJob() {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    Instant stime = Instant.now().plusSeconds(10);
    Instant etime = stime.plus(1, ChronoUnit.DAYS);
    JobDefinition jobDef = JobDefinition.newBuilder(TestJob.class)
        .startAt(stime)
        .endAt(etime)
        .fixedDelay(Duration.ofMinutes(60))
        .repeat(10)
        .build();
    scheduler.schedule(jobDef);

    String name = jobDef.getName();

    List<JobDefinition> jobs = scheduler.listJobDefinitionsByName(name);
    Assert.assertNotNull(jobs);
    Assert.assertEquals(jobs.size(), 1);
    JobDefinition job = jobs.iterator().next();
    Assert.assertEquals(job.getMode(), JobDefinition.ScheduleMode.START_AT);
    Assert.assertFalse(job.isGlobal());
    Assert.assertNull(job.getCron());
    Assert.assertEquals(job.getEndTime(), etime);
    Assert.assertEquals(job.getStartTime(), stime);
    Assert.assertNull(job.getRate());
    Assert.assertEquals(job.getDelay(), Duration.ofMinutes(60));
    Assert.assertNotNull(job.getRepeat());
    Assert.assertEquals(job.getRepeat().intValue(), 10);
  }

  @Test
  public void testUnscheduleJobNotInterrupt() {
    testPauseJob(false);
  }

  @Test
  public void testUnscheduleJobInterrupt() {
    testPauseJob(true);
  }

  @Test(expectedExceptions = JobException.class)
  public void testPauseJobTwice() {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = scheduler.runOnce(TestJob.class, time);
    Assert.assertNotNull(jobDef);

    scheduler.pause(jobDef.getId(), false);
    scheduler.pause(jobDef.getId(), true);
  }

  @Test
  public void testDeleteJob() {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = scheduler.runOnce(TestJob.class, time);
    Assert.assertNotNull(jobDef);

    scheduler.delete(jobDef.getId());

    List<JobDefinition> jobs = scheduler.listJobDefinitionsByName(jobDef.getName());
    Assert.assertNotNull(jobs);
    Assert.assertTrue(jobs.isEmpty());
  }

  private void testPauseJob(boolean interrupt) {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = scheduler.runOnce(TestJob.class, time);
    Assert.assertNotNull(jobDef);

    JobDefinition job = scheduler.pause(jobDef.getId(), interrupt);

    Assert.assertNotNull(job);
    Map<JobDefinition, JobDefinitionStatus> jobstats = scheduler.listJobDefinitionsWithStatus();
    JobDefinitionStatus status = null;
    for(Map.Entry<JobDefinition, JobDefinitionStatus> entry : jobstats.entrySet()) {
      JobDefinition j = entry.getKey();
      if (j.getId().equals(job.getId())) {
        status = entry.getValue();
        break;
      }
    }
    Assert.assertNotNull(status);
    Assert.assertEquals(status.getState(), JobDefinitionState.PAUSED);
  }
}
