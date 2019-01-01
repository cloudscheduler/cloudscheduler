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

package io.github.cloudscheduler.model;

import io.github.cloudscheduler.Job;
import io.github.cloudscheduler.JobExecutionContext;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Wei Gao
 */
public class JobDefinitionBuilderTest {
  @Test
  public void testBasic() {
    JobDefinition job = JobDefinition.newBuilder(TestJob.class)
        .build();
    Assert.assertNotNull(job.getId());
    Assert.assertEquals(job.getName(), job.getId().toString());
    Assert.assertEquals(job.getMode(), JobDefinition.ScheduleMode.START_NOW);
    Assert.assertNull(job.getStartTime());
    Assert.assertNull(job.getCron());
    Assert.assertNull(job.getRate());
    Assert.assertNull(job.getDelay());
    Assert.assertNull(job.getRepeat());
    Assert.assertNull(job.getEndTime());
  }

  @Test
  public void testDifferentOrder() {
    JobDefinition job1 = JobDefinition.newBuilder(TestJob.class)
        .name("TestJob")
        .initialDelay(Duration.ofHours(1))
        .fixedRate(Duration.ofHours(5))
        .repeat(50)
        .runNow() // Override initial delay
        .build();
    JobDefinition job2 = JobDefinition.newBuilder(TestJob.class)
        .name("TestJob")
        .fixedRate(Duration.ofHours(5))
        .repeat(50)
        .build();
    compare(job1, job2);
  }

  @Test
  public void testExclusiveRateAndDelay() {
    JobDefinition job = JobDefinition.newBuilder(TestJob.class)
        .name("TestJob")
        .initialDelay(Duration.ofHours(1))
        .fixedRate(Duration.ofHours(5))
        .fixedDelay(Duration.ofMinutes(10))
        .repeat(50)
        .runNow() // Override initial delay
        .build();
    Assert.assertNull(job.getRate());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testRepeatWithoutRateOrDelay() {
    // Repeat without interval and cron
    JobDefinition.newBuilder(TestJob.class)
        .runNow()
        .repeat(50)
        .build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIllegalRate() {
    JobDefinition.newBuilder(TestJob.class)
        .fixedRate(Duration.ofSeconds(1))
        .build();
  }

  @Test
  public void testCron() {
    JobDefinition.newBuilder(TestJob.class)
        .cron("0 0 * * *")
        .build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidCron() {
    JobDefinition.newBuilder(TestJob.class)
        .cron("0 0 a * * *")
        .build();
  }

  @Test
  public void testGlobal() {
    JobDefinition job = JobDefinition.newBuilder(TestJob.class)
        .startAt(Instant.now().plus(1, ChronoUnit.MINUTES))
        .global()
        .build();
    Assert.assertTrue(job.isGlobal());
    job = JobDefinition.newBuilder(TestJob.class)
        .jobData("test", "Test value")
        .global(false)
        .build();
    Assert.assertFalse(job.isGlobal());
  }

  @Test
  public void testAllowDuplicateInstance() {
    JobDefinition job = JobDefinition.newBuilder(TestJob.class)
        .allowDupInstances()
        .build();
    Assert.assertTrue(job.isAllowDupInstances());
    job = JobDefinition.newBuilder(TestJob.class)
        .allowDupInstances(false)
        .build();
    Assert.assertFalse(job.isAllowDupInstances());
  }

  @Test
  public void testHashCode() {
    String jobName = "jobName";
    JobDefinition job1 = JobDefinition.newBuilder(TestJob.class)
        .name(jobName)
        .build();
    JobDefinition job2 = JobDefinition.newBuilder(TestJob.class)
        .name(jobName)
        .build();
    Assert.assertNotEquals(job1.hashCode(), job2.hashCode());
  }

  @Test
  public void testEquals() {
    String jobName = "jobName";
    UUID id = UUID.randomUUID();
    JobDefinition job1 = JobDefinition.newBuilder(TestJob.class)
        .name(jobName)
        .build();
    JobDefinition job2 = JobDefinition.newBuilder(TestJob.class)
        .name(jobName)
        .build();
    Assert.assertNotEquals(job1, job2);
    job1 = new JobDefinition(id, TestJob.class, "jobName1", null,
        JobDefinition.ScheduleMode.START_AT, null, null, null, null,
        null, null, false, false);
    job2 = new JobDefinition(id, TestJob.class, "jobName2", null,
        JobDefinition.ScheduleMode.START_NOW, null, null, null, null,
        null, null, true, true);
    Assert.assertEquals(job1, job2);
  }

  @Test
  public void testToString() {
    JobDefinition job = JobDefinition.newBuilder(TestJob.class)
        .name("jobName")
        .build();
    Assert.assertNotNull(job.toString());
  }

  private void compare(JobDefinition actual, JobDefinition expected) {
    if (actual == null && expected == null) {
      return;
    }
    if (actual == null) {
      Assert.fail("Actual is null");
    }
    if (expected == null) {
      Assert.fail("Expected is null but actual is not");
    }
    Assert.assertEquals(actual.getJobClass(), expected.getJobClass());
    Assert.assertEquals(actual.getName(), expected.getName());
    Assert.assertEquals(actual.getMode(), expected.getMode());
    Assert.assertEquals(actual.getCron(), expected.getCron());
    Assert.assertEquals(actual.getStartTime(), expected.getStartTime());
    Assert.assertEquals(actual.getRate(), expected.getRate());
    Assert.assertEquals(actual.getDelay(), expected.getDelay());
    Assert.assertEquals(actual.getData(), expected.getData());
    Assert.assertEquals(actual.getRepeat(), expected.getRepeat());
    Assert.assertEquals(actual.getEndTime(), expected.getEndTime());
    Assert.assertEquals(actual.isGlobal(), expected.isGlobal());
  }

  static class TestJob implements Job {
    @Override
    public void execute(JobExecutionContext ctx) {
      // Do nothing.
    }
  }
}
