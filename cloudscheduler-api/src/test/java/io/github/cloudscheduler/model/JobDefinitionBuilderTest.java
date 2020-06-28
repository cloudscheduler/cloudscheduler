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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import io.github.cloudscheduler.Job;
import io.github.cloudscheduler.JobExecutionContext;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** @author Wei Gao */
public class JobDefinitionBuilderTest {
  @Test
  public void testBasic() {
    JobDefinition job = JobDefinition.newBuilder(TestJob.class).build();
    assertThat(job.getId()).isNotNull();
    assertThat(job.getName()).isEqualTo(job.getId().toString());
    assertThat(job.getMode()).isEqualTo(ScheduleMode.START_NOW);
    assertThat(job.getStartTime()).isNull();

    assertThat(job.getCron()).isNull();
    assertThat(job.getRate()).isNull();
    assertThat(job.getDelay()).isNull();
    assertThat(job.getRepeat()).isNull();
    assertThat(job.getEndTime()).isNull();
  }

  @Test
  public void testDifferentOrder() {
    JobDefinition job1 =
        JobDefinition.newBuilder(TestJob.class)
            .name("TestJob")
            .initialDelay(Duration.ofHours(1))
            .fixedRate(Duration.ofHours(5))
            .repeat(50)
            .runNow() // Override initial delay
            .build();
    JobDefinition job2 =
        JobDefinition.newBuilder(TestJob.class)
            .name("TestJob")
            .fixedRate(Duration.ofHours(5))
            .repeat(50)
            .build();
    assertThat(job1).isEqualToIgnoringGivenFields(job2, "id");
  }

  @Test
  public void testExclusiveRateAndDelay() {
    JobDefinition job =
        JobDefinition.newBuilder(TestJob.class)
            .name("TestJob")
            .initialDelay(Duration.ofHours(1))
            .fixedRate(Duration.ofHours(5))
            .fixedDelay(Duration.ofMinutes(10))
            .repeat(50)
            .runNow() // Override initial delay
            .build();
    assertThat(job.getRate()).isNull();
  }

  @Test
  public void testRepeatWithoutRateOrDelay() {
    assertThatIllegalStateException()
        .isThrownBy(
            () -> {
              // Repeat without interval and cron
              JobDefinition.newBuilder(TestJob.class).runNow().repeat(50).build();
            });
  }

  @Test
  public void testIllegalRate() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> JobDefinition.newBuilder(TestJob.class).fixedRate(Duration.ofSeconds(1)).build());
  }

  @Test
  public void testCron() {
    JobDefinition.newBuilder(TestJob.class).cron("0 0 * * *").build();
  }

  @Test
  public void testInvalidCron() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> JobDefinition.newBuilder(TestJob.class).cron("0 0 a * * *").build());
  }

  @Test
  public void testGlobal() {
    JobDefinition job =
        JobDefinition.newBuilder(TestJob.class)
            .startAt(Instant.now().plus(1, ChronoUnit.MINUTES))
            .global()
            .build();
    assertThat(job.isGlobal()).isTrue();
    job =
        JobDefinition.newBuilder(TestJob.class).jobData("test", "Test value").global(false).build();
    assertThat(job.isGlobal()).isFalse();
  }

  @Test
  public void testAllowDuplicateInstance() {
    JobDefinition job = JobDefinition.newBuilder(TestJob.class).allowDupInstances().build();
    assertThat(job.isAllowDupInstances()).isTrue();
    job = JobDefinition.newBuilder(TestJob.class).allowDupInstances(false).build();
    assertThat(job.isAllowDupInstances()).isFalse();
  }

  @Test
  public void testHashCode() {
    String jobName = "jobName";
    JobDefinition job1 = JobDefinition.newBuilder(TestJob.class).name(jobName).build();
    JobDefinition job2 = JobDefinition.newBuilder(TestJob.class).name(jobName).build();
    assertThat(job1.hashCode()).isNotEqualTo(job2.hashCode());
  }

  @Test
  public void testEquals() {
    String jobName = "jobName";
    UUID id = UUID.randomUUID();
    JobDefinition job1 = JobDefinition.newBuilder(TestJob.class).name(jobName).build();
    JobDefinition job2 = JobDefinition.newBuilder(TestJob.class).name(jobName).build();
    assertThat(job1).isNotEqualTo(job2);
    job1 =
        new JobDefinition(
            id,
            TestJob.class,
            "jobName1",
            null,
            ScheduleMode.START_AT,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            false);
    job2 =
        new JobDefinition(
            id,
            TestJob.class,
            "jobName2",
            null,
            ScheduleMode.START_NOW,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            true);
    assertThat(job1).isEqualTo(job2);
  }

  @Test
  public void testToString() {
    JobDefinition job = JobDefinition.newBuilder(TestJob.class).name("jobName").build();
    assertThat(job.toString()).isNotNull();
  }

  static class TestJob implements Job {
    @Override
    public void execute(JobExecutionContext ctx) {
      // Do nothing.
    }
  }
}
