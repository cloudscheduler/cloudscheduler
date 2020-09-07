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

package io.github.cloudscheduler.worker;

import io.github.cloudscheduler.AsyncService;
import io.github.cloudscheduler.CloudSchedulerObserver;
import io.github.cloudscheduler.EventType;
import io.github.cloudscheduler.JobFactory;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.NodeRole;
import io.github.cloudscheduler.ServiceAlreadyStartException;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.service.JobService;
import io.github.cloudscheduler.service.JobServiceImpl;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduler worker. Handle worker node logic.
 *
 * @author Wei Gao
 */
public class SchedulerWorker implements AsyncService {
  private static final Logger logger = LoggerFactory.getLogger(SchedulerWorker.class);
  private final ExecutorService customerThreadPool;
  private final AtomicBoolean running;
  private final AtomicBoolean jobInstanceChanged;
  private final AtomicBoolean scanning;
  private final Node node;
  private final ConcurrentMap<UUID, JobInstanceProcessor> processors;
  private final String zkUrl;
  private final int zkTimeout;
  private final CloudSchedulerObserver observer;
  private final JobFactory jobFactory;
  private ExecutorService threadPool;
  private JobService jobService;
  private CompletableFuture<Void> scanJobInsJob;
  private CompletableFuture<ZooKeeper> zkConnector;
  private final boolean zkConnectorOwner;

  /**
   * Constructor.
   *
   * @param node node
   * @param zkUrl zookeeper url
   * @param zkTimeout zookeeper timeout
   * @param customerThreadPool thread pool used to execute customer job
   * @param jobFactory job factory
   * @param observer observer
   */
  public SchedulerWorker(
      Node node,
      String zkUrl,
      int zkTimeout,
      ExecutorService customerThreadPool,
      JobFactory jobFactory,
      CloudSchedulerObserver observer) {
    Objects.requireNonNull(node, "Node is mandatory");
    Objects.requireNonNull(zkUrl, "ZooKeeper url is mandatory");
    this.node = node;
    this.zkUrl = zkUrl;
    this.zkTimeout = zkTimeout;
    this.jobFactory = jobFactory;
    this.customerThreadPool = customerThreadPool;
    running = new AtomicBoolean(false);
    jobInstanceChanged = new AtomicBoolean(true);
    scanning = new AtomicBoolean(false);
    processors = new ConcurrentHashMap<>();
    this.observer = observer;
    this.zkConnectorOwner = true;
  }

  public SchedulerWorker(
      Node node,
      CompletableFuture<ZooKeeper> zkConnector,
      ExecutorService customerThreadPool,
      JobFactory jobFactory,
      CloudSchedulerObserver observer) {
    Objects.requireNonNull(node, "Node is mandatory");
    Objects.requireNonNull(zkConnector, "ZooKeeper connector is mandatory");
    this.node = node;
    this.zkUrl = null;
    this.zkTimeout = -1;
    this.jobFactory = jobFactory;
    this.customerThreadPool = customerThreadPool;
    running = new AtomicBoolean(false);
    jobInstanceChanged = new AtomicBoolean(true);
    scanning = new AtomicBoolean(false);
    processors = new ConcurrentHashMap<>();
    this.observer = observer;
    this.zkConnector = zkConnector;
    this.zkConnectorOwner = false;
  }

  /** Start scheduler worker. */
  @Override
  public CompletableFuture<Void> startAsync() {
    if (running.compareAndSet(false, true)) {
      logger.info("Starting scheduler worker");
      jobInstanceChanged.set(true);
      scanJobInsJob = CompletableFuture.completedFuture(null);
      if (zkConnectorOwner) {
        zkConnector =
            ZooKeeperUtils.connectToZooKeeper(
                zkUrl,
                zkTimeout,
                eventType -> {
                  if (eventType == EventType.CONNECTION_LOST) {
                    lostConnection();
                  }
                });
      }
      zkConnector.thenAccept(this::initialWorker);
      return CompletableFuture.completedFuture(null);
    } else {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new ServiceAlreadyStartException(node, NodeRole.WORKER));
      return future;
    }
  }

  private void initialWorker(ZooKeeper zooKeeper) {
    threadPool =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "SchedulerWorker");
              t.setPriority(Thread.MAX_PRIORITY);
              return t;
            });
    jobService = new JobServiceImpl(zooKeeper);
    jobService
        .registerWorkerAsync(node)
        .thenAcceptAsync(
            n -> {
              observer.workerNodeUp(n.getId(), Instant.now());
              scanJobInstances();
            },
            threadPool);
  }

  @Override
  public CompletableFuture<Void> shutdownAsync() {
    if (running.compareAndSet(true, false)) {
      logger.info("Shutting down scheduler worker");
      CompletableFuture<ZooKeeper> t = zkConnector;
      if (zkConnectorOwner) {
        zkConnector.cancel(false);
      }
      zkConnector = new CompletableFuture<>();
      return t.thenComposeAsync(
              zk ->
                  scanJobInsJob
                      .exceptionally(
                          e -> {
                            logger.warn("Scan JobInstance exception", e);
                            return null;
                          })
                      .whenComplete((v, cause) -> logger.trace("Scan JobInstance complete"))
                      .thenCompose(m -> jobService.unregisterWorkerAsync(node))
                      .exceptionally(
                          cause -> {
                            logger.warn("Error happened when unregister scheduler worker", cause);
                            return null;
                          })
                      .whenComplete((v, cause) -> logger.trace("Scheduler worker unregistered"))
                      .thenComposeAsync(v -> destroyAllJobInstanceProcessor(), threadPool)
                      .exceptionally(
                          cause -> {
                            logger.debug("Destroy all job instance processor got exception", cause);
                            return null;
                          })
                      .whenComplete(
                          (v, cause) -> {
                            logger.trace("All JobInstance processor destroyed");
                            if (zkConnectorOwner && zk != null) {
                              try {
                                zk.close();
                                logger.trace("Zookeeper closed: {}", zk);
                              } catch (InterruptedException e) {
                                logger.debug("Close zookeeper for scheduler worker error", e);
                              }
                            }
                          }),
              threadPool)
          .exceptionally(
              cause -> {
                logger.trace("Error happened when shutdown scheduler worker", cause);
                return null;
              })
          .whenComplete(
              (v, cause) -> {
                logger.trace("Shutdown thread pool");
                if (threadPool != null) {
                  threadPool.shutdown();
                }
              })
          .whenComplete((v, cause) -> observer.workerNodeDown(node.getId(), Instant.now()))
          .whenComplete((v, cause) -> logger.info("SchedulerWorker {} down", node.getId()));
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  private void lostConnection() {
    if (running.get()) {
      logger.warn("SchedulerWorker lost connection");
      shutdownAsync().whenComplete((v, cause) -> start());
    }
  }

  private CompletableFuture<Void> destroyAllJobInstanceProcessor() {
    Map<UUID, JobInstanceProcessor> ps = new HashMap<>(processors);
    processors.clear();
    List<CompletableFuture<?>> fs = new ArrayList<>(ps.size());
    ps.forEach((jobDefId, processor) -> fs.add(processor.shutdownAsync()));
    return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]));
  }

  private void scanJobInstances() {
    logger.debug("Scanning job instance");
    while (jobInstanceChanged.compareAndSet(true, false)) {
      scanJobInsJob =
          scanJobInsJob.thenCompose(
              v ->
                  jobService
                      .listAllJobInstancesAsync(
                          eventType -> {
                            if (eventType == EventType.CHILD_CHANGED) {
                              if (running.get() && jobInstanceChanged.compareAndSet(false, true)) {
                                logger.trace("Start scan job instance");
                                scanJobInstances();
                              } else {
                                logger.trace("Scan job instance in progress.");
                              }
                            }
                          })
                      .thenAcceptAsync(this::processJobInstances, threadPool));
    }
  }

  private void processJobInstances(List<JobInstance> jobIns) {
    if (scanning.compareAndSet(false, true)) {
      logger.debug("Process {} JobInstance(s)", jobIns.size());
      try {
        logger.trace("JobInstance changed.");
        List<UUID> jobInIds = new ArrayList<>(jobIns.size());
        jobIns.forEach(
            jobIn -> {
              jobInIds.add(jobIn.getId());
              processors.computeIfAbsent(
                  jobIn.getId(),
                  (id -> {
                    logger.trace("Find new JobInstance, id: {}", id);
                    try {
                      JobInstanceProcessor processor =
                          new JobInstanceProcessor(
                              node,
                              jobIn,
                              zkConnector.get(),
                              threadPool,
                              customerThreadPool,
                              jobService,
                              jobFactory,
                              observer);
                      processor.start();
                      return processor;
                    } catch (Throwable e) {
                      logger.warn("Cannot get zookeeper client.", e);
                      return null;
                    }
                  }));
            });
        // Remove not exist JobInstance processor
        Iterator<Map.Entry<UUID, JobInstanceProcessor>> ite = processors.entrySet().iterator();
        while (ite.hasNext()) {
          Map.Entry<UUID, JobInstanceProcessor> entry = ite.next();
          UUID id = entry.getKey();
          JobInstanceProcessor processor = entry.getValue();
          if (!jobInIds.contains(id)) {
            logger.trace("JobInstance with id {}, gone, remove processor.", id);
            ite.remove();
            processor.shutdown();
          }
        }
        logger.trace("Process JobInstance done.");
      } finally {
        scanning.set(false);
      }
    } else {
      logger.debug("Process job instances in progress.");
    }
  }

  JobInstanceProcessor getJobProcessorById(UUID jobInId) {
    return processors.get(jobInId);
  }
}
