/*
 *
 * Copyright (c) 2020. cloudscheduler
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
 *
 */

package io.github.cloudscheduler.master;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cloudscheduler.CloudSchedulerObserver;
import io.github.cloudscheduler.EventType;
import io.github.cloudscheduler.JobFactory;
import io.github.cloudscheduler.JobScheduleCalculator;
import io.github.cloudscheduler.model.*;
import io.github.cloudscheduler.service.JobService;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import org.junit.jupiter.api.Test;

class JobDefinitionProcessorTest {
  @Injectable private JobDefinition jobDef;

  @Injectable
  private ExecutorService threadPool =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread t = new Thread(r, "SchedulerMaster");
            t.setPriority(Thread.MAX_PRIORITY);
            return t;
          });

  @Injectable private JobService jobService;
  @Injectable private JobFactory jobFactory;

  @Injectable
  private ExecutorService customerThreadPool =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r, "CustomerThreadPool");
            return t;
          });

  @Injectable private CloudSchedulerObserver observer;
  @Tested private JobDefinitionProcessor cut;

  @Test
  void testStartAsync() {
    assertThat(cut.startAsync()).isDone().isCompleted();
  }

  @Test
  void testStartAsyncWithCron() {
    new Expectations() {
      {
        jobDef.getCron();
        result = "15 23 * * *";
      }
    };
    cut =
        new JobDefinitionProcessor(
            jobDef, threadPool, jobService, jobFactory, customerThreadPool, observer);
    assertThat(cut.startAsync()).isDone().isCompleted();
  }

  @Test
  void testShutdownAsync(@Mocked JobDefinitionStatus status) {
    UUID id = UUID.randomUUID();
    new Expectations() {
      {
        jobDef.getId();
        result = id;
        jobService.getJobStatusByIdAsync(id, (Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(status);
        status.getState();
        result = JobDefinitionState.PAUSED;
        jobService.cleanUpJobInstances(jobDef);
        result = CompletableFuture.completedFuture(Collections.emptyList());
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(cut.shutdownAsync()).succeedsWithin(Duration.ofSeconds(2));
  }

  @Test
  void testScheduleJobInstance(@Mocked JobDefinitionStatus status, @Mocked JobInstance jobIns) {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
        jobService.getJobStatusByIdAsync(jobDefId, (Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(status);
        jobService.getJobStatusByIdAsync(jobDefId);
        result = CompletableFuture.completedFuture(status);
        status.getState();
        result = JobDefinitionState.CREATED;
        jobService.cleanUpJobInstances(jobDef);
        result = CompletableFuture.completedFuture(Collections.emptyList());
        jobDef.getRepeat();
        result = null;
        jobDef.getDelay();
        result = Duration.ofSeconds(10);
        jobService.scheduleJobInstanceAsync(jobDef);
        result = CompletableFuture.completedFuture(jobIns);
        jobIns.getId();
        result = jobInsId;
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(cut.shutdownAsync()).succeedsWithin(Duration.ofSeconds(2));
  }

  @Test
  void testScheduleJobInstanceCron(@Mocked JobDefinitionStatus status) {
    UUID jobDefId = UUID.randomUUID();
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
        jobService.getJobStatusByIdAsync(jobDefId, (Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(status);
        jobService.getJobStatusByIdAsync(jobDefId);
        result = CompletableFuture.completedFuture(status);
        status.getState();
        result = JobDefinitionState.CREATED;
        jobService.cleanUpJobInstances(jobDef);
        result = CompletableFuture.completedFuture(Collections.emptyList());
        jobDef.getRepeat();
        result = null;
        jobDef.getMode();
        result = ScheduleMode.CRON;
        jobDef.getCron();
        result = "* 5 * * *";
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(cut.shutdownAsync()).succeedsWithin(Duration.ofSeconds(2));
  }

  @Test
  void testScheduleJobInstanceCustomize(
      @Mocked JobDefinitionStatus status,
      @Mocked JobInstance jobIns,
      @Mocked JobScheduleCalculator calculator)
      throws Throwable {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
        jobService.getJobStatusByIdAsync(jobDefId, (Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(status);
        jobService.getJobStatusByIdAsync(jobDefId);
        result = CompletableFuture.completedFuture(status);
        status.getState();
        result = JobDefinitionState.CREATED;
        jobService.cleanUpJobInstances(jobDef);
        result = CompletableFuture.completedFuture(Collections.emptyList());
        jobDef.getRepeat();
        result = null;
        jobDef.getMode();
        result = ScheduleMode.CUSTOMIZED;
        jobDef.getCalculatorClass();
        result = JobScheduleCalculator.class;
        jobFactory.createJobScheduleCalculator((Class<? extends JobScheduleCalculator>) any);
        result = calculator;
        calculator.calculateNextRunTime(jobDef, status);
        result = Instant.now().plus(Duration.ofSeconds(5));
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(cut.shutdownAsync()).succeedsWithin(Duration.ofSeconds(2));
  }
}
