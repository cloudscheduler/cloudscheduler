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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionState;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.ScheduleMode;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SchedulerImplTest extends AbstractTest {
  @Test
  public void testScheduleRunOnce() {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    JobDefinition jobDef = scheduler.runNow(TestJob.class);
    assertThat(jobDef).isNotNull();
    assertThat(jobDef.getMode()).isEqualTo(ScheduleMode.START_NOW);
    assertThat(jobDef.isGlobal()).isFalse();
    assertThat(jobDef.getCron()).isNull();
    assertThat(jobDef.getEndTime()).isNull();
    assertThat(jobDef.getStartTime()).isNull();
    assertThat(jobDef.getDelay()).isNull();
    assertThat(jobDef.getRate()).isNull();
    assertThat(jobDef.getRepeat()).isNull();
  }

  @Test
  public void testScheduleRunNow() {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = scheduler.runOnce(TestJob.class, time);
    assertThat(jobDef).isNotNull();
    assertThat(jobDef.getMode()).isEqualTo(ScheduleMode.START_AT);
    assertThat(jobDef.isGlobal()).isFalse();
    assertThat(jobDef.getCron()).isNull();
    assertThat(jobDef.getEndTime()).isNull();
    assertThat(jobDef.getStartTime()).isEqualTo(time);
    assertThat(jobDef.getDelay()).isNull();
    assertThat(jobDef.getRate()).isNull();
    assertThat(jobDef.getRepeat()).isNull();
  }

  @Test
  public void testScheduleJob() {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    Instant stime = Instant.now().plusSeconds(10);
    Instant etime = stime.plus(1, ChronoUnit.DAYS);
    JobDefinition jobDef =
        JobDefinition.newBuilder(TestJob.class)
            .startAt(stime)
            .endAt(etime)
            .fixedDelay(Duration.ofMinutes(60))
            .repeat(10)
            .build();
    scheduler.schedule(jobDef);

    String name = jobDef.getName();

    List<JobDefinition> jobs = scheduler.listJobDefinitionsByName(name);
    assertThat(jobs).hasSize(1);
    JobDefinition job = jobs.iterator().next();
    assertThat(job.getMode()).isEqualTo(ScheduleMode.START_AT);
    assertThat(job.isGlobal()).isFalse();
    assertThat(job.getCron()).isNull();
    assertThat(job.getEndTime()).isEqualTo(etime);
    assertThat(job.getStartTime()).isEqualTo(stime);
    assertThat(job.getRate()).isNull();
    assertThat(job.getDelay()).isEqualTo(Duration.ofMinutes(60));
    assertThat(job.getRepeat()).isNotNull();
    assertThat(job.getRepeat().intValue()).isEqualTo(10);
  }

  @Test
  public void testUnscheduleJobNotInterrupt() {
    testPauseJob(false);
  }

  @Test
  public void testUnscheduleJobInterrupt() {
    testPauseJob(true);
  }

  @Test
  public void testPauseJobTwice() {
    assertThatExceptionOfType(JobException.class)
        .isThrownBy(
            () -> {
              Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

              Instant time = Instant.now().plusSeconds(10);
              JobDefinition jobDef = scheduler.runOnce(TestJob.class, time);
              assertThat(jobDef).isNotNull();

              scheduler.pause(jobDef.getId(), false);
              scheduler.pause(jobDef.getId(), true);
            });
  }

  @Test
  public void testDeleteJob() {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = scheduler.runOnce(TestJob.class, time);
    assertThat(jobDef).isNotNull();

    scheduler.delete(jobDef.getId());

    List<JobDefinition> jobs = scheduler.listJobDefinitionsByName(jobDef.getName());
    assertThat(jobs).isEmpty();
  }

  private void testPauseJob(boolean interrupt) {
    Scheduler scheduler = new SchedulerImpl(() -> zooKeeper);

    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = scheduler.runOnce(TestJob.class, time);
    assertThat(jobDef).isNotNull();

    JobDefinition job = scheduler.pause(jobDef.getId(), interrupt);

    assertThat(job).isNotNull();
    Map<JobDefinition, JobDefinitionStatus> jobstats = scheduler.listJobDefinitionsWithStatus();
    JobDefinitionStatus status = null;
    for (Map.Entry<JobDefinition, JobDefinitionStatus> entry : jobstats.entrySet()) {
      JobDefinition j = entry.getKey();
      if (j.getId().equals(job.getId())) {
        status = entry.getValue();
        break;
      }
    }
    assertThat(status).isNotNull();
    assertThat(status.getState()).isEqualTo(JobDefinitionState.PAUSED);
  }
}
