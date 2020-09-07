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
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.cloudscheduler.master.SchedulerMaster;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import io.github.cloudscheduler.worker.SchedulerWorker;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CloudSchedulerManagerTest {
  private CloudSchedulerManager cut;
  private UUID nodeId = UUID.randomUUID();
  private String zkUrl = "";
  private int zkTimeout = 200;
  @Mocked private ExecutorService customerThreadPool;
  @Mocked private JobFactory jobFactory;
  @Mocked private CloudSchedulerObserver observer;
  private NodeRole[] roles = {NodeRole.MASTER, NodeRole.WORKER};
  @Mocked private SchedulerMaster master;
  @Mocked private SchedulerWorker worker;
  @Mocked private ZooKeeper zooKeeper;

  @BeforeEach
  public void init() {
    cut =
        CloudSchedulerManager.newBuilder(zkUrl)
            .setNodeId(nodeId)
            .setZkTimeout(zkTimeout)
            .setThreadPool(customerThreadPool)
            .setJobFactory(jobFactory)
            .setObserverSupplier(() -> observer)
            .setRoles(roles)
            .build();
  }

  @Test
  public void testCreateInstanceWithDefaultValue() {
    assertThatCode(() -> CloudSchedulerManager.newBuilder(zkUrl).build())
        .doesNotThrowAnyException();
  }

  @Test
  public void testStartAsync() {
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeper> connectToZooKeeper(
          String zooKeeperUrl, int zkTimeout, Consumer<EventType> disconnectListener) {
        return CompletableFuture.completedFuture(zooKeeper);
      }
    };
    new Expectations() {
      {
        master.startAsync();
        result = CompletableFuture.completedFuture(null);
        worker.startAsync();
        result = CompletableFuture.completedFuture(null);
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
  }

  @Test
  public void testStartAsyncTwice() {
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeper> connectToZooKeeper(
          String zooKeeperUrl, int zkTimeout, Consumer<EventType> disconnectListener) {
        return CompletableFuture.completedFuture(zooKeeper);
      }
    };
    new Expectations() {
      {
        master.startAsync();
        result = CompletableFuture.completedFuture(null);
        worker.startAsync();
        result = CompletableFuture.completedFuture(null);
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(cut.startAsync())
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(ServiceAlreadyStartException.class);
  }

  @Test
  public void testShutdownAsync() throws InterruptedException {
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeper> connectToZooKeeper(
          String zooKeeperUrl, int zkTimeout, Consumer<EventType> disconnectListener) {
        return CompletableFuture.completedFuture(zooKeeper);
      }
    };
    new Expectations() {
      {
        master.startAsync();
        result = CompletableFuture.completedFuture(null);
        worker.startAsync();
        result = CompletableFuture.completedFuture(null);
        master.shutdownAsync();
        result = CompletableFuture.completedFuture(null);
        worker.shutdownAsync();
        result = CompletableFuture.completedFuture(null);
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();

    assertThat(cut.shutdownAsync()).isDone().isCompleted();
    new Verifications() {
      {
        zooKeeper.close();
      }
    };
  }

  @Test
  public void testShutdownAsyncWithErrors() throws InterruptedException {
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeper> connectToZooKeeper(
          String zooKeeperUrl, int zkTimeout, Consumer<EventType> disconnectListener) {
        return CompletableFuture.completedFuture(zooKeeper);
      }
    };
    new Expectations() {
      {
        master.startAsync();
        result = CompletableFuture.completedFuture(null);
        worker.startAsync();
        result = CompletableFuture.completedFuture(null);
        master.shutdownAsync();
        CompletableFuture<Void> masterFuture = new CompletableFuture<>();
        masterFuture.completeExceptionally(new RuntimeException());
        result = masterFuture;
        worker.shutdownAsync();
        CompletableFuture<Void> workerFuture = new CompletableFuture<>();
        workerFuture.completeExceptionally(new RuntimeException());
        result = workerFuture;
        zooKeeper.close();
        result = new InterruptedException();
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();

    assertThat(cut.shutdownAsync()).isDone().isCompleted();
  }

  @Test
  public void testShutdownAsyncWithoutStart() {
    assertThat(cut.shutdownAsync()).isDone().isCompleted();
  }

  @Test
  public void testStartAsyncMasterOnly() {
    cut =
        CloudSchedulerManager.newBuilder(zkUrl)
            .setNodeId(nodeId)
            .setZkTimeout(zkTimeout)
            .setThreadPool(customerThreadPool)
            .setJobFactory(jobFactory)
            .setObserverSupplier(() -> observer)
            .setRoles(new NodeRole[] {NodeRole.MASTER})
            .build();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeper> connectToZooKeeper(
          String zooKeeperUrl, int zkTimeout, Consumer<EventType> disconnectListener) {
        return CompletableFuture.completedFuture(zooKeeper);
      }
    };
    new Expectations() {
      {
        master.startAsync();
        result = CompletableFuture.completedFuture(null);
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    new Verifications() {
      {
        worker.startAsync();
        times = 0;
      }
    };
  }

  @Test
  public void testStartAsyncWorkerOnly() {
    cut =
        CloudSchedulerManager.newBuilder(zkUrl)
            .setNodeId(nodeId)
            .setZkTimeout(zkTimeout)
            .setThreadPool(customerThreadPool)
            .setJobFactory(jobFactory)
            .setObserverSupplier(() -> observer)
            .setRoles(new NodeRole[] {NodeRole.WORKER})
            .build();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeper> connectToZooKeeper(
          String zooKeeperUrl, int zkTimeout, Consumer<EventType> disconnectListener) {
        return CompletableFuture.completedFuture(zooKeeper);
      }
    };
    new Expectations() {
      {
        worker.startAsync();
        result = CompletableFuture.completedFuture(null);
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    new Verifications() {
      {
        master.startAsync();
        times = 0;
      }
    };
  }

  @Test
  public void testLostConnection() {
    AtomicReference<Consumer<EventType>> listener = new AtomicReference<>();
    AtomicInteger connectTimes = new AtomicInteger(0);
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeper> connectToZooKeeper(
          String zooKeeperUrl, int zkTimeout, Consumer<EventType> disconnectListener) {
        connectTimes.incrementAndGet();
        listener.set(disconnectListener);
        return CompletableFuture.completedFuture(zooKeeper);
      }
    };
    new Expectations() {
      {
        master.startAsync();
        result = CompletableFuture.completedFuture(null);
        times = 2;
        worker.startAsync();
        result = CompletableFuture.completedFuture(null);
        times = 2;
        master.shutdownAsync();
        result = CompletableFuture.completedFuture(null);
        worker.shutdownAsync();
        result = CompletableFuture.completedFuture(null);
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(listener).isNotNull();
    listener.get().accept(EventType.CONNECTION_LOST);
    assertThat(connectTimes).hasValue(2);
  }
}
