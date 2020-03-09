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
  Node registerWorker(Node node) throws Throwable;

  CompletableFuture<Node> registerWorkerAsync(Node node);

  /**
   * Unregister a worker node.
   *
   * @param node Node object
   * @return Node object
   * @throws Throwable exception
   */
  Node unregisterWorker(Node node) throws Throwable;

  CompletableFuture<Node> unregisterWorkerAsync(Node node);

  /**
   * Get list of current worker nodes.
   *
   * @return list of node id
   * @throws Throwable exception
   */
  List<UUID> getCurrentWorkers() throws Throwable;

  CompletableFuture<List<UUID>> getCurrentWorkersAsync();

  CompletableFuture<List<UUID>> getCurrentWorkersAsync(Consumer<EventType> listener);

  /**
   * Save a JobDefinition.
   *
   * @param jobDef JobDefinition to be saved.
   * @return JobDefinition
   * @throws Throwable exception
   */
  JobDefinition saveJobDefinition(JobDefinition jobDef) throws Throwable;

  CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDef);

  /**
   * Get JobDefinition by id.
   *
   * @param jobDefId JobDefinition ID
   * @return JobDefinition, {@code null} if not found
   * @throws Throwable exception
   */
  JobDefinition getJobDefinitionById(UUID jobDefId) throws Throwable;

  CompletableFuture<JobDefinition> getJobDefinitionByIdAsync(UUID jobDefId);

  /**
   * Delete a JobDefinition, this will delete all JobInstance that created from this JobDefinition
   * as well as JobDefinition status.
   * This is atomic operation, either delete all or delete none.
   *
   * @param jobDef JobDefinition
   * @throws Throwable exception
   */
  void deleteJobDefinition(JobDefinition jobDef) throws Throwable;

  CompletableFuture<Void> deleteJobDefinitionAsync(JobDefinition jobDef);

  /**
   * List all JobDefinition IDs, this API only return ID.
   *
   * @return list of JobDefinition IDs
   * @throws Throwable exception
   */
  List<UUID> listAllJobDefinitionIds() throws Throwable;

  CompletableFuture<List<UUID>> listAllJobDefinitionIdsAsync();

  /**
   * List all JobDefinitions.
   *
   * @return list of job definition
   * @throws Throwable exception
   */
  List<JobDefinition> listAllJobDefinitions() throws Throwable;

  CompletableFuture<List<JobDefinition>> listAllJobDefinitionsAsync();

  CompletableFuture<List<JobDefinition>> listAllJobDefinitionsAsync(Consumer<EventType> listener);

  /**
   * Get JobDefinitions by name.
   *
   * @param name job definition name
   * @return List of job definitions
   * @throws Throwable exception
   */
  List<JobDefinition> listJobDefinitionsByName(String name) throws Throwable;

  CompletableFuture<List<JobDefinition>> listJobDefinitionsByNameAsync(String name);

  Map<JobDefinition, JobDefinitionStatus> listJobDefinitionsWithStatus() throws Throwable;

  CompletableFuture<Map<JobDefinition, JobDefinitionStatus>> listJobDefinitionsWithStatusAsync();

  /**
   * Get JobDefinition status by id.
   * Note: JobDefinition status share same ID with JobDefinition id.
   *
   * @param id JobDefinition ID
   * @return job definition status
   * @throws Throwable exception
   */
  JobDefinitionStatus getJobStatusById(UUID id) throws Throwable;

  CompletableFuture<JobDefinitionStatus> getJobStatusByIdAsync(UUID id);

  CompletableFuture<JobDefinitionStatus> getJobStatusByIdAsync(UUID id,
                                                               Consumer<EventType> listener);

  /**
   * Get JobInstance by id.
   *
   * @param id JobInstance ID
   * @return JobInstance
   * @throws Throwable exception
   */
  JobInstance getJobInstanceById(UUID id) throws Throwable;

  CompletableFuture<JobInstance> getJobInstanceByIdAsync(UUID id);

  CompletableFuture<JobInstance> getJobInstanceByIdAsync(UUID id, Consumer<EventType> listener);

  /**
   * Delete a JobInstance by ID.
   *
   * @param id JobInstance ID
   * @throws Throwable exception
   */
  void deleteJobInstance(UUID id) throws Throwable;

  CompletableFuture<Void> deleteJobInstanceAsync(UUID jobInstanceId);

  List<UUID> listAllJobInstanceIds() throws Throwable;

  CompletableFuture<List<UUID>> listAllJobInstanceIdsAsync();

  /**
   * List all JobInstance.
   *
   * @return List of JobInstance
   * @throws Throwable exception
   */
  List<JobInstance> listAllJobInstances() throws Throwable;

  CompletableFuture<List<JobInstance>> listAllJobInstancesAsync();

  CompletableFuture<List<JobInstance>> listAllJobInstancesAsync(Consumer<EventType> listener);

  /**
   * Get all JobInstance of a JobDefinition.
   *
   * @param jobDef JobDefinition
   * @return List of JobInstance
   * @throws Throwable exception
   */
  List<JobInstance> getJobInstancesByJobDef(JobDefinition jobDef) throws Throwable;

  CompletableFuture<List<JobInstance>> getJobInstancesByJobDefAsync(JobDefinition jobDef);

  /**
   * Schedule a JobInstance from JobDefinition. Master call this API to trigger a worker start job.
   *
   * @param jobDef JobDefinition
   * @return job instance
   * @throws Throwable exception
   */
  JobInstance scheduleJobInstance(JobDefinition jobDef) throws Throwable;

  JobInstance scheduleJobInstance(JobDefinition jobDef, Instant scheduledTime) throws Throwable;

  CompletableFuture<JobInstance> scheduleJobInstanceAsync(JobDefinition jobDef);

  CompletableFuture<JobInstance> scheduleJobInstanceAsync(JobDefinition jobDef,
                                                          Instant scheduledTime);

  /**
   * Call when a node start process a job instance. This API will update both job instance and
   * job definition status running node status.
   *
   * @param jobInstanceId JobInstance id
   * @param nodeId        Which node start process it.
   * @return The JobInstance
   * @throws Throwable exception
   */
  JobInstance startProcessJobInstance(UUID jobInstanceId, UUID nodeId) throws Throwable;

  JobInstance startProcessJobInstance(UUID jobInstanceId, UUID nodeId, Instant startTime)
      throws Throwable;

  CompletableFuture<JobInstance> startProcessJobInstanceAsync(UUID jobInstanceId, UUID nodeId);

  CompletableFuture<JobInstance> startProcessJobInstanceAsync(UUID jobInstanceId, UUID nodeId,
                                                              Instant startTime);

  /**
   * Call when a node complete a job instance. This API will update job instance and
   * job definition status
   *
   * @param jobInstanceId JobInstance ID
   * @param nodeId        Node ID
   * @param state         JobInstanceState
   * @return JobInstance
   * @throws Throwable exception
   */
  JobInstance completeJobInstance(UUID jobInstanceId, UUID nodeId, JobInstanceState state)
      throws Throwable;

  JobInstance completeJobInstance(UUID jobInstanceId, UUID nodeId, Instant endTime,
                                  JobInstanceState state) throws Throwable;

  CompletableFuture<JobInstance> completeJobInstanceAsync(UUID jobInstanceId, UUID nodeId,
                                                          JobInstanceState state);

  CompletableFuture<JobInstance> completeJobInstanceAsync(UUID jobInstanceId,
                                                          UUID nodeId,
                                                          Instant endTime,
                                                          JobInstanceState state);

  /**
   * Pause a job definition, set to paused and master will not schedule it.
   * Pause an already paused or completed job will cause exception.
   *
   * @param id           JobDefinition ID
   * @param mayInterrupt if allow interrupt running job
   * @return Paused JobDefinition
   * @throws Throwable exception
   */
  JobDefinition pauseJob(UUID id, boolean mayInterrupt) throws Throwable;

  CompletableFuture<JobDefinition> pauseJobAsync(UUID id, boolean mayInterrupt);

  /**
   * Resume a paused job definition, resume and master will continue schedule it.
   * Resume a not paused job will cause exception.
   *
   * @param id           JobDefinition ID
   * @return Paused JobDefinition
   * @throws Throwable exception
   */
  JobDefinition resumeJob(UUID id) throws Throwable;

  CompletableFuture<JobDefinition> resumeJobAsync(UUID id);

  /**
   * Clean up job instances. It will check JobDefinition status, for any completed JobInstance,
   * It will check if JobInstance do not have any locker znode, when it's empty, it will go ahead
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
  JobDefinitionStatus completeJobDefinition(JobDefinition jobDef) throws Throwable;

  CompletableFuture<JobDefinitionStatus> completeJobDefinitionAsync(JobDefinition jobDef);

  String getJobInstancePath(UUID id);
}
