package io.github.cloudscheduler.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.github.cloudscheduler.EventType;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import mockit.Mocked;
import mockit.Tested;
import org.junit.jupiter.api.Test;

public class JobServiceTest {
  class MyJobService implements JobService {

    @Override
    public CompletableFuture<Node> registerWorkerAsync(Node node) {
      return CompletableFuture.completedFuture(node);
    }

    @Override
    public CompletableFuture<Node> unregisterWorkerAsync(Node node) {
      return CompletableFuture.completedFuture(node);
    }

    @Override
    public CompletableFuture<List<UUID>> getCurrentWorkersAsync() {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<UUID>> getCurrentWorkersAsync(Consumer<EventType> listener) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDef) {
      return CompletableFuture.completedFuture(jobDef);
    }

    @Override
    public CompletableFuture<JobDefinition> getJobDefinitionByIdAsync(UUID jobDefId) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteJobDefinitionAsync(JobDefinition jobDef) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<UUID>> listAllJobDefinitionIdsAsync() {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<JobDefinition>> listAllJobDefinitionsAsync() {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<JobDefinition>> listAllJobDefinitionsAsync(
        Consumer<EventType> listener) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<JobDefinition>> listJobDefinitionsByNameAsync(String name) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<Map<JobDefinition, JobDefinitionStatus>>
        listJobDefinitionsWithStatusAsync() {
      return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    @Override
    public CompletableFuture<JobDefinitionStatus> getJobStatusByIdAsync(UUID id) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<JobDefinitionStatus> getJobStatusByIdAsync(
        UUID id, Consumer<EventType> listener) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<JobInstance> getJobInstanceByIdAsync(UUID id) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<JobInstance> getJobInstanceByIdAsync(
        UUID id, Consumer<EventType> listener) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteJobInstanceAsync(UUID jobInstanceId) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<UUID>> listAllJobInstanceIdsAsync() {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<JobInstance>> listAllJobInstancesAsync() {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<JobInstance>> listAllJobInstancesAsync(
        Consumer<EventType> listener) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<JobInstance>> getJobInstancesByJobDefAsync(JobDefinition jobDef) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<JobInstance> scheduleJobInstanceAsync(JobDefinition jobDef) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<JobInstance> scheduleJobInstanceAsync(
        JobDefinition jobDef, Instant scheduledTime) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<JobInstance> startProcessJobInstanceAsync(
        UUID jobInstanceId, UUID nodeId) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<JobInstance> startProcessJobInstanceAsync(
        UUID jobInstanceId, UUID nodeId, Instant startTime) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<JobInstance> completeJobInstanceAsync(
        UUID jobInstanceId, UUID nodeId, JobInstanceState state) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<JobInstance> completeJobInstanceAsync(
        UUID jobInstanceId, UUID nodeId, Instant endTime, JobInstanceState state) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<JobDefinition> pauseJobAsync(UUID id, boolean mayInterrupt) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<JobDefinition> resumeJobAsync(UUID id) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<JobInstance>> cleanUpJobInstances(JobDefinition jobDef) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<JobDefinitionStatus> completeJobDefinitionAsync(JobDefinition jobDef) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getJobInstancePath(UUID id) {
      return null;
    }
  }

  @Tested private MyJobService cut = new MyJobService();

  @Test
  public void testRegisterWorker(@Mocked Node node) throws Throwable {
    assertThat(cut.registerWorker(node)).isEqualTo(node);
  }

  @Test
  public void testUnregisterWorker(@Mocked Node node) throws Throwable {
    assertThat(cut.unregisterWorker(node)).isEqualTo(node);
  }

  @Test
  public void testGetCurrentWorkers() throws Throwable {
    assertThat(cut.getCurrentWorkers()).isEmpty();
    ;
  }

  @Test
  public void testSaveJobDefinition(@Mocked JobDefinition jobDef) throws Throwable {
    assertThat(cut.saveJobDefinition(jobDef)).isEqualTo(jobDef);
  }

  @Test
  public void testGetJobDefinitionById() throws Throwable {
    assertThat(cut.getJobDefinitionById(UUID.randomUUID())).isNull();
  }

  @Test
  public void testDeleteJobDefinition(@Mocked JobDefinition jobDef) {
    assertThatCode(() -> cut.deleteJobDefinition(jobDef)).doesNotThrowAnyException();
  }

  @Test
  public void testListAllJobDefinitionIds() throws Throwable {
    assertThat(cut.listAllJobDefinitionIds()).isEmpty();
  }

  @Test
  public void testListAllJobDefinitions() throws Throwable {
    assertThat(cut.listAllJobDefinitions()).isEmpty();
  }

  @Test
  public void testListJobDefinitionsByName() throws Throwable {
    assertThat(cut.listJobDefinitionsByName("dummyJobDefinition")).isEmpty();
  }

  @Test
  public void testListJobDefinitionsWithStatus() throws Throwable {
    assertThat(cut.listJobDefinitionsWithStatus()).isEmpty();
  }

  @Test
  public void testGetJobStatusById() throws Throwable {
    assertThat(cut.getJobStatusById(UUID.randomUUID())).isNull();
  }

  @Test
  public void testGetJobInstanceById() throws Throwable {
    assertThat(cut.getJobInstanceById(UUID.randomUUID())).isNull();
  }

  @Test
  public void testDeleteJobInstance() {
    assertThatCode(() -> cut.deleteJobInstance(UUID.randomUUID())).doesNotThrowAnyException();
  }

  @Test
  public void testListAllJobInstanceIds() throws Throwable {
    assertThat(cut.listAllJobInstanceIds()).isEmpty();
  }

  @Test
  public void testListAllJobInstances() throws Throwable {
    assertThat(cut.listAllJobInstances()).isEmpty();
  }

  @Test
  public void testGetJobInstancesByJobDef(@Mocked JobDefinition jobDef) throws Throwable {
    assertThat(cut.getJobInstancesByJobDef(jobDef)).isEmpty();
  }

  @Test
  public void testScheduleJobInstance(@Mocked JobDefinition jobDef) throws Throwable {
    assertThat(cut.scheduleJobInstance(jobDef)).isNull();
  }

  @Test
  public void testScheduleJobInstanceWithTime(@Mocked JobDefinition jobDef) throws Throwable {
    assertThat(cut.scheduleJobInstance(jobDef, Instant.now())).isNull();
  }

  @Test
  public void testStartProcessJobInstance() throws Throwable {
    assertThat(cut.startProcessJobInstance(UUID.randomUUID(), UUID.randomUUID())).isNull();
  }

  @Test
  public void testStartProcessJobInstanceWithTime() throws Throwable {
    assertThat(cut.startProcessJobInstance(UUID.randomUUID(), UUID.randomUUID(), Instant.now()))
        .isNull();
  }

  @Test
  public void testCompleteJobInstance(@Mocked JobInstanceState state) throws Throwable {
    assertThat(cut.completeJobInstance(UUID.randomUUID(), UUID.randomUUID(), state)).isNull();
  }

  @Test
  public void testCompleteJobInstanceWithTime(@Mocked JobInstanceState state) throws Throwable {
    assertThat(cut.completeJobInstance(UUID.randomUUID(), UUID.randomUUID(), Instant.now(), state))
        .isNull();
  }

  @Test
  public void testPauseJob() throws Throwable {
    assertThat(cut.pauseJob(UUID.randomUUID(), true)).isNull();
  }

  @Test
  public void testResumeJob() throws Throwable {
    assertThat(cut.resumeJob(UUID.randomUUID())).isNull();
  }

  @Test
  public void testCompleteJobDefinition(@Mocked JobDefinition jobDef) throws Throwable {
    assertThat(cut.completeJobDefinition(jobDef)).isNull();
  }

  @Test
  public void testWrapFutureExecutionWithException() {
    assertThatExceptionOfType(IOException.class)
        .isThrownBy(
            () ->
                cut.wrapFutureExecution(
                    () -> {
                      CompletableFuture<Void> future = new CompletableFuture<>();
                      future.completeExceptionally(new IOException());
                      return future;
                    }));
  }
}
