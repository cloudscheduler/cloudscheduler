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

import io.github.cloudscheduler.master.SchedulerMaster;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import io.github.cloudscheduler.worker.SchedulerWorker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is main class to start. In order to use you Cloud scheduler, you need to create a instance
 * of this class, and call start on it. And then, use a instance of Scheduler to schedule the jobs.
 *
 * @author Wei Gao
 */
public class CloudSchedulerManager implements AsyncService {
  private static final Logger logger = LoggerFactory.getLogger(CloudSchedulerManager.class);

  private SchedulerMaster master;
  private SchedulerWorker worker;
  private final AtomicBoolean running;
  private CompletableFuture<ZooKeeper> zkConnector;
  private ZooKeeper zooKeeper;
  private final String zkUrl;
  private final int zkTimeout;
  private final List<NodeRole> roles;
  private final Node node;
  private final JobFactory jobFactory;
  private final CloudSchedulerObserver observer;
  private final ExecutorService customerThreadPool;

  /**
   * Constructor.
   *
   * @param zkUrl zookeeper url
   * @param zkTimeout zookeeper timeout value
   * @param customerThreadPool thread pool to run job
   * @param jobFactory job factory
   * @param roles node roles
   */
  private CloudSchedulerManager(
      UUID nodeId,
      String zkUrl,
      int zkTimeout,
      ExecutorService customerThreadPool,
      JobFactory jobFactory,
      CloudSchedulerObserver observer,
      List<NodeRole> roles) {
    this.node = new Node(nodeId);
    this.zkUrl = zkUrl;
    this.zkTimeout = zkTimeout;
    this.customerThreadPool = customerThreadPool;
    this.jobFactory = jobFactory;
    this.observer = observer;
    this.roles = roles;
    running = new AtomicBoolean(false);
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  /** Start this cloud scheduler manager. */
  @Override
  public CompletableFuture<Void> startAsync() {
    if (running.compareAndSet(false, true)) {
      zkConnector =
          ZooKeeperUtils.connectToZooKeeper(
                  zkUrl,
                  zkTimeout,
                  eventType -> {
                    if (eventType == EventType.CONNECTION_LOST) {
                      lostConnection();
                    }
                  })
              .whenComplete(
                  (zk, ___) -> {
                    if (zk != null) {
                      zooKeeper = zk;
                    }
                  });
      List<CompletableFuture<Void>> fs = new ArrayList<>(2);
      if (roles.contains(NodeRole.MASTER)) {
        master = new SchedulerMaster(node, zkConnector, jobFactory, customerThreadPool, observer);
        fs.add(master.startAsync());
      } else {
        master = null;
      }
      if (roles.contains(NodeRole.WORKER)) {
        worker = new SchedulerWorker(node, zkConnector, customerThreadPool, jobFactory, observer);
        fs.add(worker.startAsync());
      } else {
        worker = null;
      }
      return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]));
    } else {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new ServiceAlreadyStartException(node));
      return future;
    }
  }

  @Override
  public CompletableFuture<Void> shutdownAsync() {
    if (running.compareAndSet(true, false)) {
      logger.info("Shutting down cloud scheduler manager");
      zkConnector.cancel(false);
      List<CompletableFuture<Void>> fs = new ArrayList<>(2);
      if (worker != null) {
        fs.add(
            worker
                .shutdownAsync()
                .exceptionally(
                    cause -> {
                      logger.warn("Error happened when shutdown scheduler worker", cause);
                      return null;
                    }));
        worker = null;
      }
      if (master != null) {
        fs.add(
            master
                .shutdownAsync()
                .exceptionally(
                    cause -> {
                      logger.warn("Error happened when shutdown scheduler master", cause);
                      return null;
                    }));
        master = null;
      }
      return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
          .whenComplete(
              (__, ___) -> {
                if (zooKeeper != null) {
                  try {
                    zooKeeper.close();
                  } catch (InterruptedException e) {
                    logger.warn("Error happened when close zookeeper client", e);
                  }
                  zooKeeper = null;
                }
              });
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  private void lostConnection() {
    if (running.get()) {
      logger.warn("CloudSchedulerManager lost connection");
      shutdownAsync().thenCompose(__ -> startAsync());
    }
  }

  public static Builder newBuilder(String zkUrl) {
    Objects.requireNonNull(zkUrl, "Zookeeper URL is mandatory");
    return new Builder(zkUrl);
  }

  /** Cloud scheduler manager builder class. Used to build cloud scheduler manager. */
  public static final class Builder {
    private final String zkUrl;
    private UUID nodeId;
    private int zkTimeout;
    private ExecutorService threadPool;
    private JobFactory jobFactory;
    private Supplier<CloudSchedulerObserver> observerSupplier;
    private NodeRole[] roles;

    private Builder(String zkUrl) {
      this.zkUrl = zkUrl;
      zkTimeout = Integer.MAX_VALUE;
    }

    /**
     * Set node id.
     *
     * @param nodeId nodeId
     * @return builder
     */
    public Builder setNodeId(UUID nodeId) {
      Objects.requireNonNull(nodeId);
      this.nodeId = nodeId;
      return this;
    }

    /**
     * Set zookeeper timeout value.
     *
     * @param zkTimeout zookeeper timeout value
     * @return builder
     */
    public Builder setZkTimeout(int zkTimeout) {
      if (zkTimeout < 100) {
        throw new IllegalArgumentException("Invalid zookeeper timeout value");
      }
      this.zkTimeout = zkTimeout;
      return this;
    }

    /**
     * Set thread pool.
     *
     * @param threadPool thread pool
     * @return builder
     */
    public Builder setThreadPool(ExecutorService threadPool) {
      Objects.requireNonNull(threadPool);
      this.threadPool = threadPool;
      return this;
    }

    /**
     * Set job factory.
     *
     * @param jobFactory job factory
     * @return builder
     */
    public Builder setJobFactory(JobFactory jobFactory) {
      Objects.requireNonNull(jobFactory);
      this.jobFactory = jobFactory;
      return this;
    }

    /**
     * Set observer supplier.
     *
     * @param observerSupplier observer supplier
     * @return builder
     */
    public Builder setObserverSupplier(Supplier<CloudSchedulerObserver> observerSupplier) {
      Objects.requireNonNull(observerSupplier);
      this.observerSupplier = observerSupplier;
      return this;
    }

    /**
     * Set node roles.
     *
     * @param roles node roles
     * @return builder
     */
    public Builder setRoles(NodeRole... roles) {
      Objects.requireNonNull(roles);
      this.roles = roles;
      return this;
    }

    /**
     * Build an instance of CloudSchedulerManager.
     *
     * @return instance of CloudSchedulerManager
     */
    public CloudSchedulerManager build() {
      UUID nodeId = this.nodeId;
      if (nodeId == null) {
        nodeId = UUID.randomUUID();
      }
      ExecutorService threadPool = this.threadPool;
      if (threadPool == null) {
        final AtomicInteger threadCounter = new AtomicInteger(0);
        threadPool =
            Executors.newCachedThreadPool(
                r -> {
                  Thread t = new Thread(r);
                  t.setName("JobExecutionThreadPool-" + threadCounter.incrementAndGet());
                  return t;
                });
      }
      JobFactory jobFactory = this.jobFactory;
      if (jobFactory == null) {
        jobFactory = new SimpleJobFactory();
      }
      Supplier<CloudSchedulerObserver> observerSupplier = this.observerSupplier;
      CloudSchedulerObserver observer;
      if (observerSupplier == null) {
        observer =
            new CompositeCloudSchedulerObserver(CloudSchedulerObserver.getCloudSchedulerObserver());
      } else {
        observer =
            new CompositeCloudSchedulerObserver(Collections.singletonList(observerSupplier.get()));
      }
      List<NodeRole> roles;
      if (this.roles == null || this.roles.length == 0) {
        roles = Arrays.asList(NodeRole.MASTER, NodeRole.WORKER);
      } else {
        roles = Arrays.asList(this.roles);
      }
      return new CloudSchedulerManager(
          nodeId, zkUrl, zkTimeout, threadPool, jobFactory, observer, roles);
    }
  }
}
