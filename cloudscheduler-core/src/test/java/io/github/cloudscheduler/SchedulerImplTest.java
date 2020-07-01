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

import static org.assertj.core.api.Assertions.*;

import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionState;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.ScheduleMode;
import io.github.cloudscheduler.service.JobServiceImpl;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import mockit.*;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerImplTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerImplTest.class);

  @Injectable private ZooKeeper zooKeeper;
  @Tested private SchedulerImpl cut;

  @Test
  public void testScheduleRunNow() {
    new MockUp<JobServiceImpl>() {
      @Mock
      public CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDefinition) {
        return CompletableFuture.completedFuture(jobDefinition);
      }
    };
    JobDefinition jobDef = cut.runNow(TestJob.class);
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
  public void testScheduleRunOnce() {
    new MockUp<JobServiceImpl>() {
      @Mock
      public CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDefinition) {
        return CompletableFuture.completedFuture(jobDefinition);
      }
    };
    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = cut.runOnce(TestJob.class, time);
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
    Instant stime = Instant.now().plusSeconds(10);
    Instant etime = stime.plus(1, ChronoUnit.DAYS);
    JobDefinition jobDef =
        JobDefinition.newBuilder(TestJob.class)
            .startAt(stime)
            .endAt(etime)
            .fixedDelay(Duration.ofMinutes(60))
            .repeat(10)
            .build();
    new MockUp<JobServiceImpl>() {
      @Mock
      public CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDefinition) {
        return CompletableFuture.completedFuture(jobDefinition);
      }

      @Mock
      public CompletableFuture<List<JobDefinition>> listJobDefinitionsByNameAsync(String name) {
        return CompletableFuture.completedFuture(Arrays.asList(jobDef));
      }
    };
    cut.schedule(jobDef);

    String name = jobDef.getName();

    List<JobDefinition> jobs = cut.listJobDefinitionsByName(name);
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
  public void testListJobDefinitions(@Mocked JobDefinition job1, @Mocked JobDefinition job2) {
    new MockUp<JobServiceImpl>() {
      @Mock
      public CompletableFuture<List<JobDefinition>> listAllJobDefinitionsAsync() {
        return CompletableFuture.completedFuture(Arrays.asList(job1, job2));
      }
    };
    assertThat(cut.listJobDefinitions()).containsOnly(job1, job2);
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
    new MockUp<JobServiceImpl>() {
      private JobDefinition jobDefinition;
      private boolean paused;

      @Mock
      public CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDefinition) {
        this.jobDefinition = jobDefinition;
        return CompletableFuture.completedFuture(jobDefinition);
      }

      @Mock
      public CompletableFuture<JobDefinition> pauseJobAsync(UUID id, boolean mayInterrupt) {
        if (id.equals(jobDefinition.getId())) {
          if (paused) {
            CompletableFuture<JobDefinition> future = new CompletableFuture<>();
            future.completeExceptionally(
                new JobException("JobDefinition already completed or paused"));
            return future;
          } else {
            paused = true;
            return CompletableFuture.completedFuture(jobDefinition);
          }
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }
    };
    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = cut.runOnce(TestJob.class, time);
    assertThat(jobDef).isNotNull();
    assertThatCode(() -> cut.pause(jobDef.getId(), false)).doesNotThrowAnyException();
    assertThatExceptionOfType(JobException.class).isThrownBy(() -> cut.pause(jobDef.getId(), true));
  }

  @Test
  public void testResumeJob() {
    new MockUp<JobServiceImpl>() {
      private JobDefinition jobDefinition;
      private boolean paused;

      @Mock
      public CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDefinition) {
        this.jobDefinition = jobDefinition;
        return CompletableFuture.completedFuture(jobDefinition);
      }

      @Mock
      public CompletableFuture<JobDefinition> pauseJobAsync(UUID id, boolean mayInterrupt) {
        if (id.equals(jobDefinition.getId())) {
          paused = true;
          return CompletableFuture.completedFuture(jobDefinition);
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }

      @Mock
      public CompletableFuture<Map<JobDefinition, JobDefinitionStatus>>
          listJobDefinitionsWithStatusAsync() {
        Map<JobDefinition, JobDefinitionStatus> map = new HashMap<>();
        JobDefinitionStatus status = new JobDefinitionStatus(jobDefinition.getId());
        status.setState(paused ? JobDefinitionState.PAUSED : JobDefinitionState.CREATED);
        map.put(jobDefinition, status);
        return CompletableFuture.completedFuture(map);
      }

      @Mock
      public CompletableFuture<JobDefinition> resumeJobAsync(UUID id) {
        if (id.equals(jobDefinition.getId())) {
          paused = false;
          return CompletableFuture.completedFuture(jobDefinition);
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }
    };
    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = cut.runOnce(TestJob.class, time);
    assertThat(jobDef).isNotNull();
    assertThatCode(() -> cut.pause(jobDef.getId(), false)).doesNotThrowAnyException();
    Map<JobDefinition, JobDefinitionStatus> status = cut.listJobDefinitionsWithStatus();
    assertThat(status.get(jobDef))
        .isNotNull()
        .hasFieldOrPropertyWithValue("state", JobDefinitionState.PAUSED);
    assertThatCode(() -> cut.resume(jobDef.getId())).doesNotThrowAnyException();
    status = cut.listJobDefinitionsWithStatus();
    assertThat(status.get(jobDef))
        .isNotNull()
        .hasFieldOrPropertyWithValue("state", JobDefinitionState.CREATED);
  }

  @Test
  public void testDeleteJob() {
    new MockUp<JobServiceImpl>() {
      private JobDefinition jobDefinition;

      @Mock
      public CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDefinition) {
        this.jobDefinition = jobDefinition;
        return CompletableFuture.completedFuture(jobDefinition);
      }

      @Mock
      public CompletableFuture<JobDefinition> getJobDefinitionByIdAsync(UUID jobDefId) {
        if (jobDefId.equals(jobDefinition.getId())) {
          return CompletableFuture.completedFuture(jobDefinition);
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }

      @Mock
      public CompletableFuture<Void> deleteJobDefinitionAsync(JobDefinition jobDef) {
        if (jobDef.equals(jobDefinition)) {
          jobDefinition = null;
          return CompletableFuture.completedFuture(null);
        } else {
          CompletableFuture<Void> future = new CompletableFuture<>();
          future.completeExceptionally(new RuntimeException());
          return future;
        }
      }

      @Mock
      public CompletableFuture<List<JobDefinition>> listJobDefinitionsByNameAsync(String name) {
        return CompletableFuture.completedFuture(
            jobDefinition == null ? Collections.emptyList() : Arrays.asList(jobDefinition));
      }
    };
    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = cut.runOnce(TestJob.class, time);
    assertThat(jobDef).isNotNull();

    cut.delete(jobDef.getId());

    List<JobDefinition> jobs = cut.listJobDefinitionsByName(jobDef.getName());
    assertThat(jobs).isEmpty();
  }

  @Test
  public void testWrapExecutionWithRuntimeException() {
    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(
            () ->
                cut.wrapException(
                    () -> {
                      throw new RuntimeException();
                    }));
  }

  @Test
  public void testWrapExecutionWithException() {
    assertThatExceptionOfType(JobException.class)
        .isThrownBy(
            () ->
                cut.wrapException(
                    () -> {
                      throw new Exception("RootCause");
                    }))
        .havingCause()
        .withMessage("RootCause");
  }

  private void testPauseJob(boolean interrupt) {
    new MockUp<JobServiceImpl>() {
      private JobDefinition jobDefinition;
      private boolean paused;

      @Mock
      public CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDefinition) {
        this.jobDefinition = jobDefinition;
        return CompletableFuture.completedFuture(jobDefinition);
      }

      @Mock
      public CompletableFuture<JobDefinition> pauseJobAsync(UUID id, boolean mayInterrupt) {
        if (id.equals(jobDefinition.getId())) {
          paused = true;
          return CompletableFuture.completedFuture(jobDefinition);
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }

      @Mock
      public CompletableFuture<Map<JobDefinition, JobDefinitionStatus>>
          listJobDefinitionsWithStatusAsync() {
        Map<JobDefinition, JobDefinitionStatus> map = new HashMap<>();
        JobDefinitionStatus status = new JobDefinitionStatus(jobDefinition.getId());
        status.setState(paused ? JobDefinitionState.PAUSED : JobDefinitionState.CREATED);
        map.put(jobDefinition, status);
        return CompletableFuture.completedFuture(map);
      }
    };
    Instant time = Instant.now().plusSeconds(10);
    JobDefinition jobDef = cut.runOnce(TestJob.class, time);
    assertThat(jobDef).isNotNull();

    JobDefinition job = cut.pause(jobDef.getId(), interrupt);

    assertThat(job).isNotNull();
    Map<JobDefinition, JobDefinitionStatus> jobstats = cut.listJobDefinitionsWithStatus();
    assertThat(jobstats.get(job))
        .isNotNull()
        .hasFieldOrPropertyWithValue("state", JobDefinitionState.PAUSED);
  }
}
