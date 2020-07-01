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

package io.github.cloudscheduler.service;

import io.github.cloudscheduler.EventType;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * Centralized job related service.
 *
 * @author Wei Gao
 */
public interface JobService {
  /**
   * Register a worker node.
   *
   * @param node Node object
   * @return Node object
   * @throws Throwable exception
   */
  default Node registerWorker(Node node) throws Throwable {
    return wrapFutureExecution(() -> registerWorkerAsync(node));
  }

  CompletableFuture<Node> registerWorkerAsync(Node node);

  /**
   * Unregister a worker node.
   *
   * @param node Node object
   * @return Node object
   * @throws Throwable exception
   */
  default Node unregisterWorker(Node node) throws Throwable {
    return wrapFutureExecution(() -> unregisterWorkerAsync(node));
  }

  CompletableFuture<Node> unregisterWorkerAsync(Node node);

  /**
   * Get list of current worker nodes.
   *
   * @return list of node id
   * @throws Throwable exception
   */
  default List<UUID> getCurrentWorkers() throws Throwable {
    return wrapFutureExecution(() -> getCurrentWorkersAsync());
  }

  CompletableFuture<List<UUID>> getCurrentWorkersAsync();

  CompletableFuture<List<UUID>> getCurrentWorkersAsync(Consumer<EventType> listener);

  /**
   * Save a JobDefinition.
   *
   * @param jobDef JobDefinition to be saved.
   * @return JobDefinition
   * @throws Throwable exception
   */
  default JobDefinition saveJobDefinition(JobDefinition jobDef) throws Throwable {
    return wrapFutureExecution(() -> saveJobDefinitionAsync(jobDef));
  }

  CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDef);

  /**
   * Get JobDefinition by id.
   *
   * @param jobDefId JobDefinition ID
   * @return JobDefinition, {@code null} if not found
   * @throws Throwable exception
   */
  default JobDefinition getJobDefinitionById(UUID jobDefId) throws Throwable {
    return wrapFutureExecution(() -> getJobDefinitionByIdAsync(jobDefId));
  }

  CompletableFuture<JobDefinition> getJobDefinitionByIdAsync(UUID jobDefId);

  /**
   * Delete a JobDefinition, this will delete all JobInstance that created from this JobDefinition
   * as well as JobDefinition status. This is atomic operation, either delete all or delete none.
   *
   * @param jobDef JobDefinition
   * @throws Throwable exception
   */
  default void deleteJobDefinition(JobDefinition jobDef) throws Throwable {
    wrapFutureExecution(() -> deleteJobDefinitionAsync(jobDef));
  }

  CompletableFuture<Void> deleteJobDefinitionAsync(JobDefinition jobDef);

  /**
   * List all JobDefinition IDs, this API only return ID.
   *
   * @return list of JobDefinition IDs
   * @throws Throwable exception
   */
  default List<UUID> listAllJobDefinitionIds() throws Throwable {
    return wrapFutureExecution(() -> listAllJobDefinitionIdsAsync());
  }

  CompletableFuture<List<UUID>> listAllJobDefinitionIdsAsync();

  /**
   * List all JobDefinitions.
   *
   * @return list of job definition
   * @throws Throwable exception
   */
  default List<JobDefinition> listAllJobDefinitions() throws Throwable {
    return wrapFutureExecution(() -> listAllJobDefinitionsAsync());
  }

  CompletableFuture<List<JobDefinition>> listAllJobDefinitionsAsync();

  CompletableFuture<List<JobDefinition>> listAllJobDefinitionsAsync(Consumer<EventType> listener);

  /**
   * Get JobDefinitions by name.
   *
   * @param name job definition name
   * @return List of job definitions
   * @throws Throwable exception
   */
  default List<JobDefinition> listJobDefinitionsByName(String name) throws Throwable {
    return wrapFutureExecution(() -> listJobDefinitionsByNameAsync(name));
  }

  CompletableFuture<List<JobDefinition>> listJobDefinitionsByNameAsync(String name);

  default Map<JobDefinition, JobDefinitionStatus> listJobDefinitionsWithStatus() throws Throwable {
    return wrapFutureExecution(() -> listJobDefinitionsWithStatusAsync());
  }

  CompletableFuture<Map<JobDefinition, JobDefinitionStatus>> listJobDefinitionsWithStatusAsync();

  /**
   * Get JobDefinition status by id. Note: JobDefinition status share same ID with JobDefinition id.
   *
   * @param id JobDefinition ID
   * @return job definition status
   * @throws Throwable exception
   */
  default JobDefinitionStatus getJobStatusById(UUID id) throws Throwable {
    return wrapFutureExecution(() -> getJobStatusByIdAsync(id));
  }

  CompletableFuture<JobDefinitionStatus> getJobStatusByIdAsync(UUID id);

  CompletableFuture<JobDefinitionStatus> getJobStatusByIdAsync(
      UUID id, Consumer<EventType> listener);

  /**
   * Get JobInstance by id.
   *
   * @param id JobInstance ID
   * @return JobInstance
   * @throws Throwable exception
   */
  default JobInstance getJobInstanceById(UUID id) throws Throwable {
    return wrapFutureExecution(() -> getJobInstanceByIdAsync(id));
  }

  CompletableFuture<JobInstance> getJobInstanceByIdAsync(UUID id);

  CompletableFuture<JobInstance> getJobInstanceByIdAsync(UUID id, Consumer<EventType> listener);

  /**
   * Delete a JobInstance by ID.
   *
   * @param id JobInstance ID
   * @throws Throwable exception
   */
  default void deleteJobInstance(UUID id) throws Throwable {
    wrapFutureExecution(() -> deleteJobInstanceAsync(id));
  }

  CompletableFuture<Void> deleteJobInstanceAsync(UUID jobInstanceId);

  default List<UUID> listAllJobInstanceIds() throws Throwable {
    return wrapFutureExecution(() -> listAllJobInstanceIdsAsync());
  }

  CompletableFuture<List<UUID>> listAllJobInstanceIdsAsync();

  /**
   * List all JobInstance.
   *
   * @return List of JobInstance
   * @throws Throwable exception
   */
  default List<JobInstance> listAllJobInstances() throws Throwable {
    return wrapFutureExecution(() -> listAllJobInstancesAsync());
  }

  CompletableFuture<List<JobInstance>> listAllJobInstancesAsync();

  CompletableFuture<List<JobInstance>> listAllJobInstancesAsync(Consumer<EventType> listener);

  /**
   * Get all JobInstance of a JobDefinition.
   *
   * @param jobDef JobDefinition
   * @return List of JobInstance
   * @throws Throwable exception
   */
  default List<JobInstance> getJobInstancesByJobDef(JobDefinition jobDef) throws Throwable {
    return wrapFutureExecution(() -> getJobInstancesByJobDefAsync(jobDef));
  }

  CompletableFuture<List<JobInstance>> getJobInstancesByJobDefAsync(JobDefinition jobDef);

  /**
   * Schedule a JobInstance from JobDefinition. Master call this API to trigger a worker start job.
   *
   * @param jobDef JobDefinition
   * @return job instance
   * @throws Throwable exception
   */
  default JobInstance scheduleJobInstance(JobDefinition jobDef) throws Throwable {
    return wrapFutureExecution(() -> scheduleJobInstanceAsync(jobDef));
  }

  default JobInstance scheduleJobInstance(JobDefinition jobDef, Instant scheduledTime)
      throws Throwable {
    return wrapFutureExecution(() -> scheduleJobInstanceAsync(jobDef, scheduledTime));
  }

  CompletableFuture<JobInstance> scheduleJobInstanceAsync(JobDefinition jobDef);

  CompletableFuture<JobInstance> scheduleJobInstanceAsync(
      JobDefinition jobDef, Instant scheduledTime);

  /**
   * Call when a node start process a job instance. This API will update both job instance and job
   * definition status running node status.
   *
   * @param jobInstanceId JobInstance id
   * @param nodeId Which node start process it.
   * @return The JobInstance
   * @throws Throwable exception
   */
  default JobInstance startProcessJobInstance(UUID jobInstanceId, UUID nodeId) throws Throwable {
    return wrapFutureExecution(() -> startProcessJobInstanceAsync(jobInstanceId, nodeId));
  }

  default JobInstance startProcessJobInstance(UUID jobInstanceId, UUID nodeId, Instant startTime)
      throws Throwable {
    return wrapFutureExecution(
        () -> startProcessJobInstanceAsync(jobInstanceId, nodeId, startTime));
  }

  CompletableFuture<JobInstance> startProcessJobInstanceAsync(UUID jobInstanceId, UUID nodeId);

  CompletableFuture<JobInstance> startProcessJobInstanceAsync(
      UUID jobInstanceId, UUID nodeId, Instant startTime);

  /**
   * Call when a node complete a job instance. This API will update job instance and job definition
   * status
   *
   * @param jobInstanceId JobInstance ID
   * @param nodeId Node ID
   * @param state JobInstanceState
   * @return JobInstance
   * @throws Throwable exception
   */
  default JobInstance completeJobInstance(UUID jobInstanceId, UUID nodeId, JobInstanceState state)
      throws Throwable {
    return wrapFutureExecution(() -> completeJobInstanceAsync(jobInstanceId, nodeId, state));
  }

  default JobInstance completeJobInstance(
      UUID jobInstanceId, UUID nodeId, Instant endTime, JobInstanceState state) throws Throwable {
    return wrapFutureExecution(
        () -> completeJobInstanceAsync(jobInstanceId, nodeId, endTime, state));
  }

  CompletableFuture<JobInstance> completeJobInstanceAsync(
      UUID jobInstanceId, UUID nodeId, JobInstanceState state);

  CompletableFuture<JobInstance> completeJobInstanceAsync(
      UUID jobInstanceId, UUID nodeId, Instant endTime, JobInstanceState state);

  /**
   * Pause a job definition, set to paused and master will not schedule it. Pause an already paused
   * or completed job will cause exception.
   *
   * @param id JobDefinition ID
   * @param mayInterrupt if allow interrupt running job
   * @return Paused JobDefinition
   * @throws Throwable exception
   */
  default JobDefinition pauseJob(UUID id, boolean mayInterrupt) throws Throwable {
    return wrapFutureExecution(() -> pauseJobAsync(id, mayInterrupt));
  }

  CompletableFuture<JobDefinition> pauseJobAsync(UUID id, boolean mayInterrupt);

  /**
   * Resume a paused job definition, resume and master will continue schedule it. Resume a not
   * paused job will cause exception.
   *
   * @param id JobDefinition ID
   * @return Paused JobDefinition
   * @throws Throwable exception
   */
  default JobDefinition resumeJob(UUID id) throws Throwable {
    return wrapFutureExecution(() -> resumeJobAsync(id));
  }

  CompletableFuture<JobDefinition> resumeJobAsync(UUID id);

  /**
   * Clean up job instances. It will check JobDefinition status, for any completed JobInstance, It
   * will check if JobInstance do not have any locker znode, when it's empty, it will go ahead
   * remove JobInstance, and remove entry from JobDefinition status.
   *
   * @param jobDef JobDefinition
   * @return List of removed job instance
   */
  CompletableFuture<List<JobInstance>> cleanUpJobInstances(JobDefinition jobDef);

  /**
   * Set JobDefinition status as finished.
   *
   * @param jobDef JobDefinition
   * @return Job definition status
   * @throws Throwable exception
   */
  default JobDefinitionStatus completeJobDefinition(JobDefinition jobDef) throws Throwable {
    return wrapFutureExecution(() -> completeJobDefinitionAsync(jobDef));
  }

  CompletableFuture<JobDefinitionStatus> completeJobDefinitionAsync(JobDefinition jobDef);

  String getJobInstancePath(UUID id);

  default <T> T wrapFutureExecution(FutureExecution<T> execution) throws Throwable {
    try {
      return execution.execute().get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @FunctionalInterface
  interface FutureExecution<T> {
    CompletableFuture<T> execute();
  }
}
