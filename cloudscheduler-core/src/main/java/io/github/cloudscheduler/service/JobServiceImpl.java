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
import io.github.cloudscheduler.JobException;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.codec.EntityCodecProvider;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionState;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import io.github.cloudscheduler.model.JobRunStatus;
import io.github.cloudscheduler.util.CompletableFutureUtils;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import io.github.cloudscheduler.util.retry.RetryStrategy;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobService implementation. This class will initial zookeeper znodes in constructor.
 * Since using async zookeeper operation, it extends CompletableFuture, once it complete
 * means zookeeper znodes initialized, so we can chain other operations with it.
 *
 * @author Wei Gao
 */
public class JobServiceImpl extends CompletableFuture<Void> implements JobService {
  private static final Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);

  private static final String ZK_ROOT_KEY = "cloud.scheduler.zookeeper.chroot";
  private static final String ZK_ROOT_DEFAULT = "/scheduler";
  private static final String JOB_DEF_ROOT = "/jobDefs";
  private static final String JOB_INSTANCE_ROOT = "/jobInstances";
  private static final String WORKER_NODE_ROOT = "/workers";

  private static final String STATUS_PATH = "status";

  private final Supplier<ZooKeeper> zooKeeperSupplier;
  private final RetryStrategy retryStrategy;
  private final String jobDefRoot;
  private final String jobInstanceRoot;
  private final String workerNodeRoot;
  private final EntityCodecProvider codecProvider;

  /**
   * Constructor.
   *
   * @param zooKeeper zooKeeper
   */
  public JobServiceImpl(ZooKeeper zooKeeper) {
    this(() -> zooKeeper, RetryStrategy.newBuilder()
        .fibonacci(250L)
        .random()
        .maxDelay(3000L)
        .maxRetry(20)
        .retryOn(Collections.singletonList(KeeperException.class))
        .stopAt(Arrays.asList(KeeperException.NoAuthException.class,
            KeeperException.SessionExpiredException.class))
        .build());
  }

  /**
   * Constructor.
   *
   * @param zooKeeper zooKeeper supplier
   */
  public JobServiceImpl(Supplier<ZooKeeper> zooKeeper) {
    this(zooKeeper, RetryStrategy.newBuilder()
        .fibonacci(250L)
        .random()
        .maxDelay(3000L)
        .maxRetry(10)
        .retryOn(Collections.singletonList(KeeperException.class))
        .stopAt(Arrays.asList(KeeperException.NoAuthException.class,
            KeeperException.SessionExpiredException.class))
        .build());
  }

  private JobServiceImpl(Supplier<ZooKeeper> zooKeeperSupplier, RetryStrategy retryStrategy) {
    Objects.requireNonNull(zooKeeperSupplier, "ZooKeeper is mandatory");
    Objects.requireNonNull(retryStrategy, "RetryStrategy is mandatory");
    logger.trace("New JobServiceImpl instance with zk: {}", zooKeeperSupplier.get());
    this.zooKeeperSupplier = zooKeeperSupplier;
    this.retryStrategy = retryStrategy;
    String zkRoot = System.getProperty(ZK_ROOT_KEY, ZK_ROOT_DEFAULT);
    jobDefRoot = zkRoot + JOB_DEF_ROOT;
    jobInstanceRoot = zkRoot + JOB_INSTANCE_ROOT;
    workerNodeRoot = zkRoot + WORKER_NODE_ROOT;
    this.codecProvider = EntityCodecProvider.getCodecProvider();
    CompletableFuture.allOf(ZooKeeperUtils.createZnodes(zooKeeperSupplier.get(), jobDefRoot),
        ZooKeeperUtils.createZnodes(zooKeeperSupplier.get(), jobInstanceRoot),
        ZooKeeperUtils.createZnodes(zooKeeperSupplier.get(), workerNodeRoot))
        .whenComplete((v, cause) -> {
          if (cause != null) {
            this.completeExceptionally(cause);
          } else {
            this.complete(null);
          }
        });
  }

  @Override
  public Node registerWorker(Node node) throws Throwable {
    try {
      return registerWorkerAsync(node).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<Node> registerWorkerAsync(Node node) {
    logger.debug("Register worker node: {}", node.getId());
    return retryOperation(() -> ZooKeeperUtils.exists(zooKeeperSupplier.get(),
        getWorkerNodePath(node.getId()))
        .thenCompose(version -> {
          if (version == null) {
            return ZooKeeperUtils.createEphemeralZnode(zooKeeperSupplier.get(),
                getWorkerNodePath(node.getId()), null)
                .thenApply(s -> node);
          } else {
            return CompletableFuture.completedFuture(node);
          }
        }));
  }

  @Override
  public Node unregisterWorker(Node node) throws Throwable {
    try {
      return unregisterWorkerAsync(node).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<Node> unregisterWorkerAsync(Node node) {
    logger.debug("Unregister worker node: {}", node.getId());
    return retryOperation(() -> ZooKeeperUtils.exists(zooKeeperSupplier.get(),
        getWorkerNodePath(node.getId()))
        .thenCompose(version -> {
          if (version != null) {
            return ZooKeeperUtils.deleteIfExists(zooKeeperSupplier.get(),
                getWorkerNodePath(node.getId()))
                .thenApply(s -> node);
          } else {
            return CompletableFuture.completedFuture(node);
          }
        }));
  }

  @Override
  public List<UUID> getCurrentWorkers() throws Throwable {
    try {
      return getCurrentWorkersAsync(null).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<List<UUID>> getCurrentWorkersAsync() {
    return getCurrentWorkersAsync(null);
  }

  @Override
  public CompletableFuture<List<UUID>> getCurrentWorkersAsync(Consumer<EventType> listener) {
    return retryOperation(() -> ZooKeeperUtils.getChildren(zooKeeperSupplier.get(),
        workerNodeRoot, listener)
        .thenApply(children -> {
          List<UUID> nodes = new ArrayList<>(children.size());
          children.forEach(c -> nodes.add(UUID.fromString(c)));
          return nodes;
        }));
  }

  @Override
  public JobDefinition saveJobDefinition(JobDefinition jobDef) throws Throwable {
    try {
      return saveJobDefinitionAsync(jobDef).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<JobDefinition> saveJobDefinitionAsync(JobDefinition jobDef) {
    Objects.requireNonNull(jobDef, "JobDefinition is mandatory");
    logger.debug("Saving job definition with id: {}", jobDef.getId());
    return retryOperation(() -> ZooKeeperUtils.exists(zooKeeperSupplier.get(),
        getJobDefPath(jobDef.getId()))
        .thenCompose(version -> {
          if (version != null) {
            return CompletableFutureUtils.exceptionalCompletableFuture(
                new IllegalArgumentException("JobDefinition with id "
                    + jobDef.getId() + " already exist."));
          } else {
            return ZooKeeperUtils.transactionalOperation(zooKeeperSupplier.get(), transaction -> {
              try {
                JobDefinitionStatus jobDefinitionStatus = new JobDefinitionStatus(jobDef.getId());
                transaction.create(getJobDefPath(jobDef.getId()),
                    codecProvider.getEntityEncoder(JobDefinition.class).encode(jobDef),
                    ZooKeeperUtils.DEFAULT_ACL, CreateMode.PERSISTENT);
                transaction.create(getJobDefStatusPath(jobDefinitionStatus.getId()),
                    codecProvider.getEntityEncoder(JobDefinitionStatus.class)
                        .encode(jobDefinitionStatus),
                    ZooKeeperUtils.DEFAULT_ACL, CreateMode.PERSISTENT);
                return CompletableFuture.completedFuture(jobDef);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
          }
        }));
  }

  @Override
  public JobDefinition getJobDefinitionById(UUID id) throws Throwable {
    try {
      return getJobDefinitionByIdAsync(id).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<JobDefinition> getJobDefinitionByIdAsync(UUID id) {
    Objects.requireNonNull(id, "JobDefinition ID is mandatory");
    logger.debug("Getting job definition by id: {}", id);
    return retryOperation(() -> ZooKeeperUtils.readEntity(zooKeeperSupplier.get(),
        getJobDefPath(id), codecProvider.getEntityDecoder(JobDefinition.class)))
        .thenApply(n -> n == null ? null : n.getEntity());
  }

  @Override
  public void deleteJobDefinition(JobDefinition jobDef) throws Throwable {
    try {
      deleteJobDefinitionAsync(jobDef).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<Void> deleteJobDefinitionAsync(JobDefinition jobDef) {
    Objects.requireNonNull(jobDef, "JobDefinition ID is mandatory");
    return retryOperation(() -> ZooKeeperUtils.exists(zooKeeperSupplier.get(),
        getJobDefPath(jobDef.getId()))
        .thenCompose(version -> {
          if (version != null) {
            return ZooKeeperUtils.transactionalOperation(zooKeeperSupplier.get(), transaction -> {
              CompletableFuture<Void> removeInstanceFuture = getJobInstancesByJobDefAsync(jobDef)
                  .thenCompose(jobIns -> {
                    List<CompletableFuture<Void>> fs = new ArrayList<>(jobIns.size());
                    jobIns.forEach(jobIn -> fs.add(ZooKeeperUtils.exists(zooKeeperSupplier.get(),
                        getJobInstancePath(jobIn.getId()))
                        .thenAccept(v -> {
                          if (v != null) {
                            transaction.delete(getJobInstancePath(jobIn.getId()), v);
                          }
                        })));
                    return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]));
                  });
              CompletableFuture<Void> removeStatusFuture = ZooKeeperUtils.exists(
                  zooKeeperSupplier.get(), getJobDefStatusPath(jobDef.getId()))
                  .thenAccept(v -> {
                    if (v != null) {
                      transaction.delete(getJobDefStatusPath(jobDef.getId()), v);
                    }
                  });
              return removeInstanceFuture.thenCombine(removeStatusFuture, (v1, v2) -> {
                transaction.delete(getJobDefPath(jobDef.getId()), version);
                return null;
              });
            });
          } else {
            return CompletableFuture.completedFuture(null);
          }
        }));
  }

  @Override
  public JobDefinitionStatus getJobStatusById(UUID id) throws Throwable {
    try {
      return getJobStatusByIdAsync(id, null).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<JobDefinitionStatus> getJobStatusByIdAsync(UUID id) {
    return getJobStatusByIdAsync(id, null);
  }

  @Override
  public CompletableFuture<JobDefinitionStatus> getJobStatusByIdAsync(
      UUID id, Consumer<EventType> listener) {
    Objects.requireNonNull(id, "JobDefinitionStatus ID is mandatory");
    logger.debug("Getting JobDefinition status by id: {}", id);
    return retryOperation(() -> ZooKeeperUtils.readEntity(zooKeeperSupplier.get(),
        getJobDefStatusPath(id), codecProvider.getEntityDecoder(JobDefinitionStatus.class),
        listener)).thenApply(n -> n == null ? null : n.getEntity());
  }

  @Override
  public JobInstance getJobInstanceById(UUID id) throws Throwable {
    try {
      return getJobInstanceByIdAsync(id, null).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<JobInstance> getJobInstanceByIdAsync(UUID id) {
    return getJobInstanceByIdAsync(id, null);
  }

  @Override
  public CompletableFuture<JobInstance> getJobInstanceByIdAsync(UUID id,
                                                                Consumer<EventType> listener) {
    Objects.requireNonNull(id, "JobInstance ID is mandatory");
    logger.debug("Getting JobInstance by id: {}", id);
    return retryOperation(() -> ZooKeeperUtils.readEntity(zooKeeperSupplier.get(),
        getJobInstancePath(id), codecProvider.getEntityDecoder(JobInstance.class), listener)
        .thenApply(n -> n == null ? null : n.getEntity()));
  }

  @Override
  public void deleteJobInstance(UUID jobInstanceId) throws Throwable {
    try {
      deleteJobInstanceAsync(jobInstanceId).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<Void> deleteJobInstanceAsync(UUID jobInstanceId) {
    Objects.requireNonNull(jobInstanceId, "JobDefinition ID is mandatory");
    logger.debug("Deleting JobInstance by id: ", jobInstanceId);
    return retryOperation(() -> ZooKeeperUtils.deleteIfExists(zooKeeperSupplier.get(),
        getJobInstancePath(jobInstanceId)));
  }

  @Override
  public List<UUID> listAllJobInstanceIds() throws Throwable {
    try {
      return listAllJobInstanceIdsAsync().get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<List<UUID>> listAllJobInstanceIdsAsync() {
    return retryOperation(() -> ZooKeeperUtils.getChildren(zooKeeperSupplier.get(), jobInstanceRoot)
        .thenApply(list -> {
          List<UUID> result = new ArrayList<>(list.size());
          list.forEach(s -> result.add(UUID.fromString(s)));
          return result;
        }));
  }

  @Override
  public List<JobInstance> listAllJobInstances() throws Throwable {
    try {
      return listJobInstancesAsync(null, null).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<List<JobInstance>> listAllJobInstancesAsync() {
    return listJobInstancesAsync(null, null);
  }

  @Override
  public CompletableFuture<List<JobInstance>> listAllJobInstancesAsync(
      Consumer<EventType> listener) {
    return listJobInstancesAsync(null, listener);
  }

  @Override
  public List<JobInstance> getJobInstancesByJobDef(JobDefinition jobDef) throws Throwable {
    try {
      return getJobInstancesByJobDefAsync(jobDef).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<List<JobInstance>> getJobInstancesByJobDefAsync(JobDefinition jobDef) {
    return listJobInstancesAsync(job -> jobDef.getId().equals(job.getJobDefId()), null);
  }

  private CompletableFuture<List<JobInstance>> listJobInstancesAsync(Predicate<JobInstance> filter,
                                                                     Consumer<EventType> listener) {
    logger.debug("Listing JobInstance{}", filter == null ? " with filter" : "");
    return retryOperation(() -> ZooKeeperUtils.getChildren(zooKeeperSupplier.get(),
        jobInstanceRoot, listener)
        .thenCompose(list -> {
          logger.trace("List JobInstance get total {} records", list.size());
          List<JobInstance> result = new ArrayList<>(list.size());
          List<CompletableFuture<Void>> fs = new ArrayList<>(list.size());
          list.forEach(s -> {
            UUID id = UUID.fromString(s);
            fs.add(ZooKeeperUtils.readEntity(zooKeeperSupplier.get(), getJobInstancePath(id),
                codecProvider.getEntityDecoder(JobInstance.class))
                .thenApply(n -> n == null ? null : n.getEntity())
                .thenAccept(j -> {
                  if (j != null && (filter == null || filter.test(j))) {
                    result.add(j);
                  }
                }));
          });
          return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
              .thenApply(v -> result);
        }));
  }

  @Override
  public List<UUID> listAllJobDefinitionIds() throws Throwable {
    try {
      return listAllJobDefinitionIdsAsync().get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<List<UUID>> listAllJobDefinitionIdsAsync() {
    return retryOperation(() -> ZooKeeperUtils.getChildren(zooKeeperSupplier.get(), jobDefRoot)
        .thenApply(list -> {
          List<UUID> result = new ArrayList<>(list.size());
          list.forEach(s -> result.add(UUID.fromString(s)));
          return result;
        }));
  }

  @Override
  public List<JobDefinition> listJobDefinitionsByName(String name) throws Throwable {
    try {
      return listJobDefinitionsByNameAsync(name).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<List<JobDefinition>> listJobDefinitionsByNameAsync(String name) {
    return listJobDefinitionsAsync(job -> name.equals(job.getName()), null);
  }

  @Override
  public Map<JobDefinition, JobDefinitionStatus> listJobDefinitionsWithStatus() throws Throwable {
    try {
      return listJobDefinitionsWithStatusAsync().get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<Map<JobDefinition, JobDefinitionStatus>>
      listJobDefinitionsWithStatusAsync() {
    return listJobDefinitionsAsync(null, null).thenCompose(js -> {
      Map<JobDefinition, JobDefinitionStatus> jobs = new HashMap<>(js.size());
      List<CompletableFuture<?>> fs = new ArrayList<>(js.size());
      js.forEach(j -> fs.add(getJobStatusByIdAsync(j.getId()).thenAccept(s -> jobs.put(j, s))));
      return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
          .thenApply(v -> jobs);
    });
  }

  @Override
  public List<JobDefinition> listAllJobDefinitions() throws Throwable {
    try {
      return listAllJobDefinitionsAsync().get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<List<JobDefinition>> listAllJobDefinitionsAsync() {
    return listJobDefinitionsAsync(null, null);
  }

  @Override
  public CompletableFuture<List<JobDefinition>> listAllJobDefinitionsAsync(
      Consumer<EventType> listener) {
    return listJobDefinitionsAsync(null, listener);
  }

  private CompletableFuture<List<JobDefinition>> listJobDefinitionsAsync(
      Predicate<JobDefinition> filter,
      Consumer<EventType> listener) {
    logger.debug("Listing JobDefinition{}", filter == null ? " with filter" : "");
    return retryOperation(() -> ZooKeeperUtils.getChildren(zooKeeperSupplier.get(), jobDefRoot,
        listener)
        .thenCompose(list -> {
          logger.trace("List JobDefinition get total {} records", list.size());
          List<JobDefinition> result = new ArrayList<>(list.size());
          List<CompletableFuture<Void>> fs = new ArrayList<>(list.size());
          list.forEach(s -> {
            UUID id = UUID.fromString(s);
            fs.add(ZooKeeperUtils.readEntity(zooKeeperSupplier.get(), getJobDefPath(id),
                codecProvider.getEntityDecoder(JobDefinition.class))
                .thenApply(n -> n == null ? null : n.getEntity())
                .thenAccept(j -> {
                  if (j != null && (filter == null || filter.test(j))) {
                    logger.trace("Adding JobDefinition with id: {} into return list.", j.getId());
                    result.add(j);
                  }
                }));
          });
          return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
              .thenApply(v -> result);
        }));
  }

  @Override
  public JobInstance scheduleJobInstance(JobDefinition jobDef) throws Throwable {
    return scheduleJobInstance(jobDef, Instant.now());
  }

  @Override
  public JobInstance scheduleJobInstance(JobDefinition jobDef, Instant scheduledTime)
      throws Throwable {
    try {
      return scheduleJobInstanceAsync(jobDef, scheduledTime).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<JobInstance> scheduleJobInstanceAsync(JobDefinition jobDef) {
    return scheduleJobInstanceAsync(jobDef, Instant.now());
  }

  @Override
  public CompletableFuture<JobInstance> scheduleJobInstanceAsync(JobDefinition jobDef,
                                                                 Instant scheduledTime) {
    Objects.requireNonNull(jobDef, "JobDefinition is mandatory");
    Objects.requireNonNull(scheduledTime, "ScheduledTime is mandatory");
    logger.debug("Scheduling JobInstance for JobDefinition with id: {} at {}", jobDef,
        scheduledTime);
    return retryOperation(() -> ZooKeeperUtils.readEntity(zooKeeperSupplier.get(),
        getJobDefStatusPath(jobDef.getId()),
        codecProvider.getEntityDecoder(JobDefinitionStatus.class))
        .thenCompose(jsh -> {
          if (jsh != null) {
            return ZooKeeperUtils.transactionalOperation(zooKeeperSupplier.get(), transaction -> {
              JobInstance jobInstance = new JobInstance(jobDef.getId());
              jobInstance.setScheduledTime(scheduledTime);
              jobInstance.setJobState(JobInstanceState.SCHEDULED);
              CompletableFuture<Void> f;
              if (jobDef.isGlobal()) {
                f = getCurrentWorkersAsync().thenAccept(nodeIds -> nodeIds.forEach(nodeId ->
                    jobInstance.getRunStatus().put(nodeId,
                        new JobRunStatus(nodeId))));
              } else {
                f = CompletableFuture.completedFuture(null);
              }
              return f.thenCompose(v -> {
                try {
                  logger.trace("Create JobInstance, JobInstance id: {}", jobInstance.getId());
                  transaction.create(getJobInstancePath(jobInstance.getId()),
                      codecProvider.getEntityEncoder(JobInstance.class)
                          .encode(jobInstance),
                      ZooKeeperUtils.DEFAULT_ACL, CreateMode.PERSISTENT);
                  JobDefinitionStatus status = jsh.getEntity();
                  status.getJobInstanceState().put(jobInstance.getId(),
                      JobInstanceState.SCHEDULED);
                  status.setLastScheduleTime(scheduledTime);
                  status.setRunCount(status.getRunCount() + 1);
                  transaction.setData(getJobDefStatusPath(status.getId()),
                      codecProvider.getEntityEncoder(JobDefinitionStatus.class)
                          .encode(status), jsh.getVersion());
                  return CompletableFuture.completedFuture(jobInstance);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
            });
          } else {
            logger.trace("Cannot find JobDefinition status with id: {}", jobDef);
            return CompletableFutureUtils.exceptionalCompletableFuture(
                new IllegalArgumentException("Cannot find job definition status by id: " + jobDef));
          }
        }));
  }

  @Override
  public JobInstance startProcessJobInstance(UUID jobInstanceId, UUID nodeId) throws Throwable {
    return startProcessJobInstance(jobInstanceId, nodeId, Instant.now());
  }

  @Override
  public JobInstance startProcessJobInstance(UUID jobInstanceId, UUID nodeId, Instant startTime)
      throws Throwable {
    try {
      return startProcessJobInstanceAsync(jobInstanceId, nodeId, startTime).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<JobInstance> startProcessJobInstanceAsync(UUID jobInstanceId,
                                                                     UUID nodeId) {
    return startProcessJobInstanceAsync(jobInstanceId, nodeId, Instant.now());
  }

  @Override
  public CompletableFuture<JobInstance> startProcessJobInstanceAsync(UUID jobInstanceId,
                                                                     UUID nodeId,
                                                                     Instant startTime) {
    Objects.requireNonNull(jobInstanceId, "JobInstance ID is mandatory");
    Objects.requireNonNull(nodeId, "Node ID is mandatory");
    Objects.requireNonNull(startTime, "StartTime is mandatory");
    logger.debug("Node: {} start process JobInstance {} at {}", nodeId, jobInstanceId, startTime);
    return retryOperation(() ->
        ZooKeeperUtils.readEntity(zooKeeperSupplier.get(), getJobInstancePath(jobInstanceId),
            codecProvider.getEntityDecoder(JobInstance.class))
            .thenCompose(jih -> {
              if (jih == null) {
                return CompletableFutureUtils.exceptionalCompletableFuture(
                    new IllegalStateException("Cannot bind job instance by id: " + jobInstanceId));
              } else {
                JobInstance instance = jih.getEntity();
                Map<UUID, JobRunStatus> map = instance.getRunStatus();
                JobRunStatus status = map.computeIfAbsent(nodeId, JobRunStatus::new);
                status.setState(JobInstanceState.RUNNING);
                status.setStartTime(startTime);
                status.setFinishTime(null);
                return ZooKeeperUtils.updateEntity(zooKeeperSupplier.get(),
                    getJobInstancePath(instance.getId()), instance,
                    codecProvider.getEntityEncoder(JobInstance.class), jih.getVersion());
              }
            }));
  }

  @Override
  public JobInstance completeJobInstance(UUID jobInstanceId, UUID nodeId, JobInstanceState state)
      throws Throwable {
    return completeJobInstance(jobInstanceId, nodeId, Instant.now(), state);
  }

  @Override
  public JobInstance completeJobInstance(UUID jobInstanceId,
                                         UUID nodeId,
                                         Instant endTime,
                                         JobInstanceState state) throws Throwable {
    try {
      return completeJobInstanceAsync(jobInstanceId, nodeId, endTime, state).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<JobInstance> completeJobInstanceAsync(UUID jobInstanceId,
                                                                 UUID nodeId,
                                                                 JobInstanceState state) {
    return completeJobInstanceAsync(jobInstanceId, nodeId, Instant.now(), state);
  }

  @Override
  public CompletableFuture<JobInstance> completeJobInstanceAsync(UUID jobInstanceId,
                                                                 UUID nodeId,
                                                                 Instant endTime,
                                                                 JobInstanceState state) {
    Objects.requireNonNull(jobInstanceId, "JobInstance ID is mandatory");
    Objects.requireNonNull(nodeId, "Node ID is mandatory");
    Objects.requireNonNull(endTime, "EndTime is mandatory");
    logger.debug("Node: {} complete process JobInstance {} at {}, state {}",
        nodeId, jobInstanceId, endTime, state);
    return retryOperation(() ->
        ZooKeeperUtils.readEntity(zooKeeperSupplier.get(), getJobInstancePath(jobInstanceId),
            codecProvider.getEntityDecoder(JobInstance.class))
            .thenCompose(jih -> {
              if (jih == null) {
                return CompletableFutureUtils.exceptionalCompletableFuture(
                    new IllegalArgumentException("Cannot find JobInstance by id: "
                        + jobInstanceId));
              }
              logger.trace("Got JobInstance by id: {}, update it.", jobInstanceId);
              JobInstance instance = jih.getEntity();
              return ZooKeeperUtils.readEntity(zooKeeperSupplier.get(),
                  getJobDefPath(instance.getJobDefId()),
                  codecProvider.getEntityDecoder(JobDefinition.class))
                  .thenCompose(jdh -> {
                    if (jdh == null) {
                      return CompletableFutureUtils.exceptionalCompletableFuture(new
                          IllegalStateException(
                          "Cannot find JobDefinition for JobInstance, instance id: "
                              + jobInstanceId + ", definition id: "
                              + instance.getJobDefId()));
                    }
                    JobDefinition jobDef = jdh.getEntity();
                    JobRunStatus status = instance.getRunStatus().get(nodeId);
                    if (status == null) {
                      return CompletableFutureUtils.exceptionalCompletableFuture(new
                          IllegalStateException("JobInstance("
                          + jobInstanceId + ") for node(" + nodeId
                          + ") doesn't exist"));
                    }
                    if (status.getState().isComplete(jobDef.isGlobal())) {
                      logger.trace("JobInstance {}, Node: {} is already complete.",
                          jobInstanceId, nodeId);
                      return CompletableFuture.completedFuture(null);
                    }
                    status.setState(state);
                    status.setFinishTime(endTime);
                    boolean global = jobDef.isGlobal();
                    boolean complete = global;
                    for (JobRunStatus s : instance.getRunStatus().values()) {
                      boolean c = s.getState().isComplete(global);
                      if (!c && global) {
                        complete = false;
                        break;
                      } else if (c && !global) {
                        complete = true;
                        break;
                      }
                    }
                    if (complete) {
                      instance.setJobState(JobInstanceState.COMPLETE);
                      return ZooKeeperUtils.readEntity(zooKeeperSupplier.get(),
                          getJobDefStatusPath(instance.getJobDefId()),
                          codecProvider.getEntityDecoder(JobDefinitionStatus.class))
                          .thenCompose(jsh -> {
                            if (jsh == null) {
                              return CompletableFutureUtils
                                  .exceptionalCompletableFuture(new
                                      IllegalArgumentException("Cannot find JobDefinition "
                                      + "status by id: " + jobInstanceId));
                            }
                            JobDefinitionStatus jobDefinitionStatus = jsh.getEntity();
                            JobInstanceState s = jobDefinitionStatus.getJobInstanceState()
                                .get(instance.getId());
                            if (s == null) {
                              return CompletableFutureUtils
                                  .exceptionalCompletableFuture(new
                                      IllegalStateException("JobInstance state not exist in "
                                      + "JobDefinition jobDefinitionStatus."
                                      + " JobInstance id: " + jobInstanceId));
                            }
                            jobDefinitionStatus.getJobInstanceState().put(instance.getId(),
                                JobInstanceState.COMPLETE);
                            jobDefinitionStatus.setLastCompleteTime(endTime);
                            return ZooKeeperUtils
                                .transactionalOperation(zooKeeperSupplier.get(),
                                    transaction -> {
                                      try {
                                        transaction.setData(
                                            getJobInstancePath(instance.getId()),
                                            codecProvider
                                                .getEntityEncoder(
                                                    JobInstance.class)
                                                .encode(instance),
                                            jih.getVersion());
                                        transaction.setData(
                                            getJobDefStatusPath(
                                                jobDefinitionStatus.getId()),
                                            codecProvider
                                                .getEntityEncoder(
                                                    JobDefinitionStatus
                                                        .class)
                                                .encode(jobDefinitionStatus),
                                            jsh.getVersion());
                                        return CompletableFuture
                                            .completedFuture(instance);
                                      } catch (IOException e) {
                                        return CompletableFutureUtils
                                            .exceptionalCompletableFuture(e);
                                      }
                                    });
                          });
                    } else {
                      return ZooKeeperUtils.updateEntity(zooKeeperSupplier.get(),
                          getJobInstancePath(instance.getId()), instance,
                          codecProvider.getEntityEncoder(JobInstance.class),
                          jih.getVersion());
                    }
                  });
            }));
  }

  @Override
  public JobDefinition pauseJob(UUID id, boolean mayInterrupt) throws Throwable {
    try {
      return pauseJobAsync(id, mayInterrupt).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<JobDefinition> pauseJobAsync(UUID id, boolean mayInterrupt) {
    Objects.requireNonNull(id, "JobDefinition ID is mandatory");
    logger.debug("Cancelling JobDefinition with id: {}", id);
    return retryOperation(() ->
        ZooKeeperUtils.readEntity(zooKeeperSupplier.get(), getJobDefPath(id),
            codecProvider.getEntityDecoder(JobDefinition.class))
            .thenCompose(jdh -> {
              if (jdh != null) {
                JobDefinition jobDef = jdh.getEntity();
                return ZooKeeperUtils.readEntity(zooKeeperSupplier.get(), getJobDefStatusPath(id),
                    codecProvider.getEntityDecoder(JobDefinitionStatus.class))
                    .thenCompose(jsh -> {
                      if (jsh != null) {
                        JobDefinitionStatus jobDefinitionStatus = jsh.getEntity();
                        if (jobDefinitionStatus.getState().isActive()) {
                          jobDefinitionStatus.setState(JobDefinitionState.PAUSED);
                          return ZooKeeperUtils.updateEntity(zooKeeperSupplier.get(),
                              getJobDefStatusPath(jobDefinitionStatus.getId()),
                              jobDefinitionStatus,
                              codecProvider.getEntityEncoder(
                                  JobDefinitionStatus.class),
                              jsh.getVersion())
                              .thenApply(s -> jobDef);
                        } else {
                          return CompletableFutureUtils.exceptionalCompletableFuture(
                              new JobException("JobDefinition already completed or paused"));
                        }
                      } else {
                        return CompletableFutureUtils
                            .exceptionalCompletableFuture(new IllegalArgumentException(
                                "Cannot find JobDefinition status by id: " + id));
                      }
                    });
              } else {
                return CompletableFutureUtils.exceptionalCompletableFuture(
                    new IllegalArgumentException("Cannot find JobDefinition by id: " + id));
              }
            })
    );
  }

  @Override
  public JobDefinition resumeJob(UUID id) throws Throwable {
    try {
      return resumeJobAsync(id).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<JobDefinition> resumeJobAsync(UUID id) {
    Objects.requireNonNull(id, "JobDefinition ID is mandatory");
    logger.debug("Cancelling JobDefinition with id: {}", id);
    return retryOperation(() ->
        ZooKeeperUtils.readEntity(zooKeeperSupplier.get(), getJobDefPath(id),
            codecProvider.getEntityDecoder(JobDefinition.class))
            .thenCompose(jdh -> {
              if (jdh != null) {
                JobDefinition jobDef = jdh.getEntity();
                return ZooKeeperUtils.readEntity(zooKeeperSupplier.get(), getJobDefStatusPath(id),
                    codecProvider.getEntityDecoder(JobDefinitionStatus.class))
                    .thenCompose(jsh -> {
                      if (jsh != null) {
                        JobDefinitionStatus jobDefinitionStatus = jsh.getEntity();
                        if (jobDefinitionStatus.getState().equals(JobDefinitionState.PAUSED)) {
                          jobDefinitionStatus.setState(JobDefinitionState.CREATED);
                          return ZooKeeperUtils.updateEntity(zooKeeperSupplier.get(),
                              getJobDefStatusPath(jobDefinitionStatus.getId()),
                              jobDefinitionStatus,
                              codecProvider.getEntityEncoder(
                                  JobDefinitionStatus.class),
                              jsh.getVersion())
                              .thenApply(s -> jobDef);
                        } else {
                          return CompletableFutureUtils.exceptionalCompletableFuture(
                              new JobException("JobDefinition or paused"));
                        }
                      } else {
                        return CompletableFutureUtils
                            .exceptionalCompletableFuture(new IllegalArgumentException(
                                "Cannot find JobDefinition status by id: " + id));
                      }
                    });
              } else {
                return CompletableFutureUtils.exceptionalCompletableFuture(
                    new IllegalArgumentException("Cannot find JobDefinition by id: " + id));
              }
            })
    );
  }

  @Override
  public CompletableFuture<List<JobInstance>> cleanUpJobInstances(JobDefinition jobDef) {
    Objects.requireNonNull(jobDef, "JobDefinition is mandatory");
    logger.debug("Cleanup JobInstances for JobDefinition: {}", jobDef.getId());
    return retryOperation(() -> ZooKeeperUtils.readEntity(zooKeeperSupplier.get(),
        getJobDefStatusPath(jobDef.getId()),
        codecProvider.getEntityDecoder(JobDefinitionStatus.class))
        .thenCompose(jsh -> {
          if (jsh == null) {
            return CompletableFutureUtils.exceptionalCompletableFuture(
                new IllegalArgumentException("Cannot find JobDefinition status by id: "
                    + jobDef.getId()));
          }
          List<UUID> completeJobInstance = new ArrayList<>();
          jsh.getEntity().getJobInstanceState().forEach((id, state) -> {
            logger.trace("Processing JobInstance: {}", id);
            if (state.isComplete(jobDef.isGlobal())) {
              logger.trace("JobInstance: {} complete", id);
              completeJobInstance.add(id);
            }
          });
          if (!completeJobInstance.isEmpty()) {
            List<CompletableFuture<Void>> fs = new ArrayList<>(completeJobInstance.size());
            List<JobInstance> instances = new ArrayList<>(completeJobInstance.size());
            completeJobInstance.forEach(id -> fs.add(removeJobInstance(id)
                .thenAccept(instances::add)));
            return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
                .thenApply(v -> instances);
          } else {
            return CompletableFuture.completedFuture(Collections.emptyList());
          }
        }));
  }

  private CompletableFuture<JobInstance> removeJobInstance(UUID id) {
    CompletableFuture<JobInstance> future = new CompletableFuture<>();
    removeJobInstance(id, future);
    return future;
  }

  private void removeJobInstance(UUID id, CompletableFuture<JobInstance> future) {
    logger.trace("Remove JobInstance: {}", id);
    ZooKeeperUtils.getChildren(zooKeeperSupplier.get(), getJobInstancePath(id), eventType -> {
      switch (eventType) {
        case CHILD_CHANGED:
          logger.trace("Children changed, reprocess remove JobInstance");
          removeJobInstance(id, future);
          break;
        default:
          break;
      }
    }).thenAccept(children -> {
      if (children.isEmpty()) {
        logger.trace("JobInstance {} has no children, remove it", id);
        ZooKeeperUtils.readEntity(zooKeeperSupplier.get(), getJobInstancePath(id),
            codecProvider.getEntityDecoder(JobInstance.class))
            .thenCompose(jih -> {
              if (jih != null) {
                JobInstance instance = jih.getEntity();
                return ZooKeeperUtils.readEntity(zooKeeperSupplier.get(),
                    getJobDefStatusPath(instance.getJobDefId()),
                    codecProvider.getEntityDecoder(JobDefinitionStatus.class))
                    .thenCompose(jsh -> {
                      if (jsh != null) {
                        JobDefinitionStatus jobDefinitionStatus = jsh.getEntity();
                        logger.trace("Remove JobInstance {} for "
                                + "JobDefinition jobDefinitionStatus {}",
                            id, jobDefinitionStatus.getId());
                        jobDefinitionStatus.getJobInstanceState().remove(id);
                        return ZooKeeperUtils.transactionalOperation(zooKeeperSupplier.get(),
                            transaction -> {
                              try {
                                transaction.setData(getJobDefStatusPath(
                                    jobDefinitionStatus.getId()),
                                    codecProvider.getEntityEncoder(JobDefinitionStatus.class)
                                        .encode(jobDefinitionStatus), jsh.getVersion());
                                transaction.delete(getJobInstancePath(id), jih.getVersion());
                                return CompletableFuture.completedFuture(instance);
                              } catch (IOException e) {
                                return CompletableFutureUtils.exceptionalCompletableFuture(e);
                              }
                            });
                      } else {
                        return CompletableFuture.completedFuture(instance);
                      }
                    });
              } else {
                return CompletableFuture.completedFuture(null);
              }
            }).thenAccept(future::complete)
            .exceptionally(cause -> {
              future.completeExceptionally(cause);
              return null;
            });
      } else {
        logger.trace("JobInstance {} has children, wait for children changed event.", id);
      }
    })
        .exceptionally(cause -> {
          logger.debug("Error happened when process remove JobInstance: {}", id);
          future.completeExceptionally(cause);
          return null;
        });
  }

  @Override
  public JobDefinitionStatus completeJobDefinition(JobDefinition jobDef) throws Throwable {
    try {
      return completeJobDefinitionAsync(jobDef).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public CompletableFuture<JobDefinitionStatus> completeJobDefinitionAsync(JobDefinition jobDef) {
    Objects.requireNonNull(jobDef, "JobDefinition is mandatory");
    logger.debug("Complete JobDefinition: {}", jobDef.getId());
    return retryOperation(() -> ZooKeeperUtils.readEntity(zooKeeperSupplier.get(),
        getJobDefStatusPath(jobDef.getId()),
        codecProvider.getEntityDecoder(JobDefinitionStatus.class))
        .thenCompose(jsh -> {
          if (jsh == null) {
            return CompletableFutureUtils.exceptionalCompletableFuture(
                new IllegalArgumentException("Cannot find JobDefinition status by id: "
                    + jobDef.getId()));
          }
          JobDefinitionStatus jobDefinitionStatus = jsh.getEntity();
          logger.trace("Set jobDefinitionStatus state to FINISHED");
          jobDefinitionStatus.setState(JobDefinitionState.FINISHED);
          return ZooKeeperUtils.updateEntity(zooKeeperSupplier.get(),
              getJobDefStatusPath(jobDef.getId()), jobDefinitionStatus,
              codecProvider.getEntityEncoder(JobDefinitionStatus.class), jsh.getVersion());
        }));
  }

  private String getJobDefPath(UUID id) {
    return jobDefRoot + "/" + id.toString();
  }

  private String getJobDefStatusPath(UUID id) {
    return getJobDefPath(id) + "/" + STATUS_PATH;
  }

  @Override
  public String getJobInstancePath(UUID id) {
    return jobInstanceRoot + "/" + id.toString();
  }

  private String getWorkerNodePath(UUID id) {
    return workerNodeRoot + "/" + id.toString();
  }

  /**
   * Retry logic. Retry based on exception type.
   * Will retry if exception is KeeperException and not NOAUTH or SESSIONEXPIRED exception.
   * If function throw any other exception, or KeeperException.NOAUTH or
   * KeeperException.SESSIONEXPIRED it will not retry
   *
   * @param supplier User provide function. Need to return a CompletableFuture
   * @param <T>      Entity type
   * @return CompletableFuture
   */
  private <T> CompletableFuture<T> retryOperation(Supplier<CompletableFuture<T>> supplier) {
    return thenCompose(v -> retryStrategy.call(supplier));
  }
}
