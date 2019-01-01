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

package io.github.cloudscheduler.master;

import io.github.cloudscheduler.AsyncService;
import io.github.cloudscheduler.CloudSchedulerObserver;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import io.github.cloudscheduler.service.JobService;
import io.github.cloudscheduler.service.JobServiceImpl;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import io.github.cloudscheduler.util.lock.DistributedLock;
import io.github.cloudscheduler.util.lock.DistributedLockImpl;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduler master. Handle master node logic.
 *
 * @author Wei Gao
 */
public class SchedulerMaster extends CompletableFuture<Void> implements AsyncService {
  private static final Logger logger = LoggerFactory.getLogger(SchedulerMaster.class);
  private static final String LOCK_PATH = "lock";
  private static final String MASTER_LOCK = "master";
  private final AtomicBoolean running;
  private final AtomicBoolean jobDefinitionChanged;
  private final AtomicBoolean scanningJobDefinitions;
  private final AtomicBoolean workerNodeChanged;
  private final AtomicBoolean scanningWorkerNodes;
  private final Node node;
  private final ConcurrentMap<UUID, JobDefinitionProcessor> processors;
  private final Set<UUID> workerNodes;
  private final String zkUrl;
  private final int zkTimeout;
  private final CloudSchedulerObserver observer;
  private ExecutorService threadPool;
  private ScheduledExecutorService scheduledThreadPool;
  private JobService jobService;
  private DistributedLock masterLock;
  private CompletableFuture<?> scanJobDefJob;
  private CompletableFuture<?> scanWorkerNodeJob;
  private CompletableFuture<ZooKeeper> zkConnector;

  /**
   * Constructor.
   *
   * @param node      node
   * @param zkUrl     zookeeper url
   * @param zkTimeout zookeeper timeout value
   * @param observer  observer
   */
  public SchedulerMaster(Node node,
                         String zkUrl,
                         int zkTimeout,
                         CloudSchedulerObserver observer) {
    Objects.requireNonNull(node, "Node is mandatory");
    Objects.requireNonNull(zkUrl, "ZooKeeper url is mandatory");
    Objects.requireNonNull(observer, "Observer is mandatory");
    this.node = node;
    this.zkUrl = zkUrl;
    this.zkTimeout = zkTimeout;
    this.observer = observer;
    running = new AtomicBoolean(false);
    jobDefinitionChanged = new AtomicBoolean(true);
    scanningJobDefinitions = new AtomicBoolean(false);
    workerNodeChanged = new AtomicBoolean(true);
    scanningWorkerNodes = new AtomicBoolean(false);
    processors = new ConcurrentHashMap<>();
    workerNodes = new HashSet<>();
  }

  /**
   * Start scheduler master.
   */
  public void start() {
    if (running.compareAndSet(false, true)) {
      logger.info("Starting scheduler master");
      zkConnector = new CompletableFuture<>();
      scanWorkerNodeJob = CompletableFuture.completedFuture(null);
      scanJobDefJob = CompletableFuture.completedFuture(null);
      workerNodeChanged.set(true);
      jobDefinitionChanged.set(true);
      zkConnector = ZooKeeperUtils.connectToZooKeeper(zkUrl, zkTimeout, eventType -> {
        switch (eventType) {
          case CONNECTION_LOST:
            lostConnection();
            break;
          default:
            break;
        }
      });
      zkConnector.thenAccept(this::initialMaster);
    }
  }

  private void initialMaster(ZooKeeper zooKeeper) {
    threadPool = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "SchedulerMaster");
      t.setPriority(Thread.MAX_PRIORITY);
      return t;
    });
    scheduledThreadPool = Executors.newScheduledThreadPool(1, r -> {
      Thread t = new Thread(r, "SchedulerMaster-Scheduler");
      t.setPriority(Thread.MAX_PRIORITY);
      return t;
    });
    logger.trace("Master zookeeper: {}", zooKeeper);
    jobService = new JobServiceImpl(zooKeeper);
    masterLock = new DistributedLockImpl(this.node.getId(), zooKeeper, LOCK_PATH, MASTER_LOCK);
    masterLock.lock().thenAccept(v -> {
      logger.info("I'm the master now.");
      observer.masterNodeUp(node.getId(), Instant.now());
      scanWorkerNodes();
      scanJobDefinitions();
    }).whenComplete((v, cause) -> {
      if (cause != null) {
        logger.warn("SchedulerMaster start throw exception", cause);
        completeExceptionally(cause);
      } else {
        logger.trace("SchedulerMaster started.");
        complete(null);
      }
    });
  }

  @Override
  public CompletableFuture<Void> shutdownAsync() {
    if (running.compareAndSet(true, false)) {
      logger.info("Shutting down scheduler master");
      zkConnector.cancel(false);
      CompletableFuture<ZooKeeper> t = zkConnector;
      zkConnector = new CompletableFuture<>();
      return t.exceptionally(e -> {
        logger.trace("ZooKeeper exception: ", e);
        return null;
      }).thenCompose(zk ->
          scanJobDefJob.exceptionally(e -> {
            logger.warn("Scan JobDefinition exception", e);
            return null;
          })
              .whenComplete((v, cause) -> logger.info("Scan JobDefinition complete"))
              .thenCombine(scanWorkerNodeJob.exceptionally(e -> {
                logger.warn("Scan worker node exception", e);
                return null;
              }).whenComplete((v, cause) -> logger.info("Scan worker node complete")),
                  (a, b) -> null)
              .thenCompose(v -> destroyAllJobDefinitionProcessor())
              .exceptionally(e -> {
                logger.warn("Destroy all JobDefinition processor exception", e);
                return null;
              }).whenComplete((v, cause) -> logger.info("All JobDefinition processor destroyed"))
              .thenCompose(v -> masterLock.unlock())
              .exceptionally(e -> {
                logger.warn("Error happened when unlock master lock", e);
                return null;
              })
              .whenComplete((v, cause) -> {
                if (zk != null) {
                  logger.debug("Close zookeeper {}", zk);
                  try {
                    zk.close();
                  } catch (InterruptedException e) {
                    logger.debug("Close zookeeper for scheduler master error", e);
                  }
                }
              })
      ).exceptionally(e -> {
        logger.trace("Error when shutdown scheduler master.", e);
        return null;
      })
          .whenComplete((v, cause) -> {
            logger.trace("Shutdown thread pool");
            if (threadPool != null) {
              threadPool.shutdown();
            }
            logger.trace("Shutdown scheduled thread pool");
            if (scheduledThreadPool != null) {
              scheduledThreadPool.shutdown();
            }
          }).whenComplete((v, cause) ->
              observer.masterNodeDown(node.getId(), Instant.now())
          ).whenComplete((v, cause) -> logger.info("SchedulerMaster down"));
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  private void lostConnection() {
    if (running.get()) {
      logger.warn("SchedulerMaster lost connection");
      shutdownAsync().whenComplete((v, cause) -> start());
    }
  }

  private void scanWorkerNodes() {
    logger.debug("Scanning worker node, workerNodeChanged value: {}", workerNodeChanged.get());
    while (workerNodeChanged.compareAndSet(true, false)) {
      logger.trace("In scan worker node loop.");
      scanWorkerNodeJob = scanWorkerNodeJob.thenCompose(v ->
          jobService.getCurrentWorkersAsync(eventType -> {
            switch (eventType) {
              case CHILD_CHANGED:
                if (running.get() && workerNodeChanged.compareAndSet(false, true)) {
                  logger.trace("Start scan worker node");
                  scanWorkerNodes();
                } else {
                  logger.trace("Scan worker node in progress.");
                }
                break;
              default:
                break;
            }
          }).thenComposeAsync(nodes -> {
            logger.trace("Scan worker nodes get {} records", nodes.size());
            return processWorkerNodes(nodes)
                .thenApply(n -> {
                  Set<UUID> previousNodes = new HashSet<>(workerNodes);
                  previousNodes.removeAll(nodes);
                  workerNodes.addAll(nodes);
                  workerNodes.removeIf(id -> !nodes.contains(id));
                  if (!previousNodes.isEmpty()) {
                    Instant now = Instant.now();
                    logger.debug("{} worker nodes removed.", previousNodes.size());
                    previousNodes.forEach(id -> observer.workerNodeRemoved(id, now));
                  }
                  return previousNodes.size();
                });
          }, threadPool)
              .exceptionally(cause -> {
                logger.warn("Error happened when scan worker nodes", cause);
                return null;
              }));
    }
  }

  private CompletableFuture<Void> processWorkerNodes(List<UUID> workerNodeIds) {
    Instant now = Instant.now();
    if (scanningWorkerNodes.compareAndSet(false, true)) {
      logger.debug("Process {} worker node(s)", workerNodeIds.size());
      return processRemoveWorkerNodes(workerNodeIds, now).whenComplete((v, cause) -> {
        if (cause != null) {
          logger.debug("Error happened when process remove worker nodes.", cause);
        }
        scanningWorkerNodes.set(false);
      }).thenAccept(n -> logger.debug("{} job instances failed because of node down.", n));
    } else {
      logger.debug("Process worker nodes in progress.");
      return CompletableFuture.completedFuture(null);
    }
  }

  private CompletableFuture<Integer> processRemoveWorkerNodes(List<UUID> workerNodeIds,
                                                              Instant removeTime) {
    logger.trace("Process remove worker nodes");
    AtomicInteger failedJobInstances = new AtomicInteger(0);
    return jobService.listAllJobInstanceIdsAsync().thenComposeAsync(ids -> {
      List<CompletableFuture<JobInstance>> fs = new ArrayList<>();
      ids.forEach(id -> fs.add(jobService.getJobInstanceByIdAsync(id)
          .thenComposeAsync(ji -> {
            if (ji != null) {
              for (UUID nodeId : ji.getRunStatus().keySet()) {
                if (!workerNodeIds.contains(nodeId)) {
                  return jobService.completeJobInstanceAsync(id,
                      nodeId, removeTime, JobInstanceState.NODE_FAILED);
                }
              }
              return CompletableFuture.completedFuture(ji);
            } else {
              return CompletableFuture.completedFuture(null);
            }
          }, threadPool)));
      return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
          .thenAccept(v -> failedJobInstances.addAndGet(fs.size()));
    }, threadPool)
        .thenApply(v -> failedJobInstances.get());
  }

  private void scanJobDefinitions() {
    logger.debug("Scanning job definition");
    while (jobDefinitionChanged.compareAndSet(true, false)) {
      scanJobDefJob = scanJobDefJob.thenCompose(v ->
          jobService.listAllJobDefinitionsAsync(eventType -> {
            switch (eventType) {
              case CHILD_CHANGED:
                if (running.get() && jobDefinitionChanged.compareAndSet(false, true)) {
                  logger.trace("Start scan job definition");
                  threadPool.submit(this::scanJobDefinitions);
                } else {
                  logger.trace("Scan job definition in progress.");
                }
                break;
              default:
                break;
            }
          }).thenAcceptAsync(this::processJobDefinitions, threadPool)
              .exceptionally(cause -> {
                logger.warn("Error happened when scan job definition", cause);
                return null;
              }));
    }
  }

  private void processJobDefinitions(List<JobDefinition> jobDefs) {
    if (scanningJobDefinitions.compareAndSet(false, true)) {
      Instant now = Instant.now();
      logger.debug("Process {} JobDefinition(s)", jobDefs.size());
      try {
        List<UUID> jobDefIds = new ArrayList<>(jobDefs.size());
        jobDefs.forEach(jobDef -> {
          jobDefIds.add(jobDef.getId());
          processors.computeIfAbsent(jobDef.getId(), (id -> {
            logger.trace("Find new JobDefinition, id: {}", id);
            JobDefinitionProcessor processor = new JobDefinitionProcessor(jobDef,
                threadPool, jobService, observer);
            processor.start();
            return processor;
          }));
        });
        // Remove not exist JobDefinition processor
        Iterator<Map.Entry<UUID, JobDefinitionProcessor>> ite = processors.entrySet().iterator();
        while (ite.hasNext()) {
          Map.Entry<UUID, JobDefinitionProcessor> entry = ite.next();
          UUID id = entry.getKey();
          JobDefinitionProcessor processor = entry.getValue();
          if (!jobDefIds.contains(id)) {
            logger.trace("JobDefinition with id {}, gone, remove processor.", id);
            ite.remove();
            processor.shutdown();
            observer.jobDefinitionRemoved(id, now);
          }
        }
        logger.trace("Process JobDefinition done.");
      } finally {
        scanningJobDefinitions.set(false);
      }
    } else {
      logger.debug("Process job definitions in progress.");
    }
  }

  private CompletableFuture<Void> destroyAllJobDefinitionProcessor() {
    Map<UUID, JobDefinitionProcessor> ps = new HashMap<>(processors);
    processors.clear();
    List<CompletableFuture<Void>> fs = new ArrayList<>(ps.size());
    ps.forEach((jobDefId, processor) -> fs.add(processor.shutdownAsync()));
    return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]));
  }

  JobDefinitionProcessor getJobProcessorById(UUID jobDefId) {
    return processors.get(jobDefId);
  }
}
