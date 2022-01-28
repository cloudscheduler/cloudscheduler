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
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.cloudscheduler.AbstractCloudSchedulerObserver;
import io.github.cloudscheduler.CloudSchedulerObserver;
import io.github.cloudscheduler.EventType;
import io.github.cloudscheduler.JobFactory;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.ServiceAlreadyStartException;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.service.JobServiceImpl;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import io.github.cloudscheduler.util.lock.DistributedLockImpl;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Tested;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SchedulerMasterTest {
  @Tested private SchedulerMaster cut;
  @Mocked private ZooKeeper zooKeeper;

  private CompletableFuture<ZooKeeper> zkConnector = CompletableFuture.completedFuture(zooKeeper);

  private String zkUrl = "localhost";
  private int zkTimeout = 100;
  private Node node = new Node(UUID.randomUUID());
  @Mocked private JobFactory jobFactory;
  private ExecutorService customerThreadPool = Executors.newCachedThreadPool();
  private CloudSchedulerObserver observer = new AbstractCloudSchedulerObserver() {};
  @Mocked private JobServiceImpl jobService;
  @Mocked private DistributedLockImpl lock;

  @BeforeEach
  public void init() {
    cut = new SchedulerMaster(node, zkConnector, jobFactory, customerThreadPool, observer);
  }

  @Test
  public void testStartAsync() {
    assertThat(cut.startAsync()).isDone().isCompleted();
  }

  @Test
  public void testDifferentConstructor() {
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeper> connectToZooKeeper(
          String zooKeeperUrl, int zkTimeout, Consumer<EventType> disconnectListener) {
        return CompletableFuture.completedFuture(zooKeeper);
      }
    };
    assertThatCode(
            () ->
                new SchedulerMaster(
                    node, zkUrl, zkTimeout, jobFactory, customerThreadPool, observer))
        .doesNotThrowAnyException();
    assertThat(cut.startAsync()).isDone().isCompleted();
  }

  @Test
  public void testStartTwice() {
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(cut.startAsync())
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(ServiceAlreadyStartException.class);
  }

  @Test
  public void testInitMaster() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(4);
    new MockUp<AbstractCloudSchedulerObserver>() {
      @Mock
      public void masterNodeUp(UUID nodeId, Instant time) {
        countDownLatch.countDown();
      }
    };
    new MockUp<DistributedLockImpl>() {
      @Mock
      public CompletableFuture<Void> lock() {
        countDownLatch.countDown();
        return CompletableFuture.completedFuture(null);
      }
    };
    new MockUp<JobServiceImpl>() {
      @Mock
      public CompletableFuture<List<UUID>> getCurrentWorkersAsync(Consumer<EventType> listener) {
        countDownLatch.countDown();
        return CompletableFuture.completedFuture(Collections.emptyList());
      }

      @Mock
      public CompletableFuture<List<JobDefinition>> listAllJobDefinitionsAsync(
          Consumer<EventType> listener) {
        countDownLatch.countDown();
        return CompletableFuture.completedFuture(Collections.emptyList());
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void testInitMasterAndShutDown() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    new MockUp<AbstractCloudSchedulerObserver>() {
      @Mock
      public void masterNodeUp(UUID nodeId, Instant time) {
        countDownLatch.countDown();
      }
    };
    new Expectations() {
      {
        lock.lock();
        result = CompletableFuture.completedFuture(null);
        lock.unlock();
        result = CompletableFuture.completedFuture(null);

        jobService.getCurrentWorkersAsync((Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(Collections.emptyList());
        jobService.listAllJobDefinitionsAsync((Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(Collections.emptyList());
        jobService.listAllJobInstanceIdsAsync();
        result = CompletableFuture.completedFuture(Collections.emptyList());
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(cut.shutdownAsync()).succeedsWithin(Duration.ofSeconds(2));
  }

  @Test
  public void testZooKeeperLostConnection() throws InterruptedException, TimeoutException {
    cut = new SchedulerMaster(node, zkUrl, zkTimeout, jobFactory, customerThreadPool, observer);
    AtomicReference<Consumer<EventType>> listener = new AtomicReference<>();
    AtomicReference<CountDownLatch> counter = new AtomicReference<>(new CountDownLatch(1));
    new MockUp<AbstractCloudSchedulerObserver>() {
      @Mock
      public void masterNodeUp(UUID nodeId, Instant time) {
        counter.get().countDown();
      }
    };
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeper> connectToZooKeeper(
          String zooKeeperUrl, int zkTimeout, Consumer<EventType> disconnectListener) {
        listener.set(disconnectListener);
        return CompletableFuture.completedFuture(zooKeeper);
      }
    };
    new Expectations() {
      {
        jobService.getCurrentWorkersAsync((Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(Collections.emptyList());
        jobService.listAllJobDefinitionsAsync((Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(Collections.emptyList());
        jobService.listAllJobInstanceIdsAsync();
        result = CompletableFuture.completedFuture(Collections.emptyList());
        lock.lock();
        result = CompletableFuture.completedFuture(null);
        lock.unlock();
        result = CompletableFuture.completedFuture(null);
      }
    };
    assertThatCode(
            () ->
                new SchedulerMaster(
                    node, zkUrl, zkTimeout, jobFactory, customerThreadPool, observer))
        .doesNotThrowAnyException();
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(counter.get().await(2, TimeUnit.SECONDS)).isTrue();
    counter.set(new CountDownLatch(1));
    assertThat(listener.get()).isNotNull();
    listener.get().accept(EventType.CONNECTION_LOST);
    assertThat(counter.get().await(2, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void testInitMasterScanWorkerException() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    new MockUp<AbstractCloudSchedulerObserver>() {
      @Mock
      public void masterNodeUp(UUID nodeId, Instant time) {
        countDownLatch.countDown();
      }
    };
    new Expectations() {
      {
        lock.lock();
        result = CompletableFuture.completedFuture(null);
        lock.unlock();
        result = CompletableFuture.completedFuture(null);

        jobService.getCurrentWorkersAsync((Consumer<EventType>) any);
        result = exceptionalCompletableFuture(new Exception());
        jobService.listAllJobDefinitionsAsync((Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(Collections.emptyList());
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(cut.shutdownAsync()).succeedsWithin(Duration.ofSeconds(2));
  }

  @Test
  public void testInitMasterScanJobDefinitionException() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    new MockUp<AbstractCloudSchedulerObserver>() {
      @Mock
      public void masterNodeUp(UUID nodeId, Instant time) {
        countDownLatch.countDown();
      }
    };
    new Expectations() {
      {
        lock.lock();
        result = CompletableFuture.completedFuture(null);
        lock.unlock();
        result = CompletableFuture.completedFuture(null);

        jobService.getCurrentWorkersAsync((Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(Collections.emptyList());
        jobService.listAllJobDefinitionsAsync((Consumer<EventType>) any);
        result = exceptionalCompletableFuture(new Exception());
        jobService.listAllJobInstanceIdsAsync();
        result = CompletableFuture.completedFuture(Collections.emptyList());
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(cut.shutdownAsync()).succeedsWithin(Duration.ofSeconds(2));
  }

  @Test
  public void testScanWorkerNodes() throws InterruptedException {
    AtomicReference<Consumer<EventType>> eventListener = new AtomicReference<>();
    CountDownLatch countDownLatch = new CountDownLatch(2);
    AtomicInteger scanTimes = new AtomicInteger(0);
    new MockUp<AbstractCloudSchedulerObserver>() {
      @Mock
      public void masterNodeUp(UUID nodeId, Instant time) {
        countDownLatch.countDown();
      }
    };
    new MockUp<JobServiceImpl>() {
      @Mock
      public CompletableFuture<List<UUID>> getCurrentWorkersAsync(Consumer<EventType> listener) {
        eventListener.set(listener);
        countDownLatch.countDown();
        scanTimes.incrementAndGet();
        return CompletableFuture.completedFuture(Collections.emptyList());
      }
    };
    new Expectations() {
      {
        lock.lock();
        result = CompletableFuture.completedFuture(null);
        lock.unlock();
        result = CompletableFuture.completedFuture(null);

        jobService.listAllJobDefinitionsAsync((Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(Collections.emptyList());
        jobService.listAllJobInstanceIdsAsync();
        result = CompletableFuture.completedFuture(Collections.emptyList());
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(eventListener.get()).isNotNull();
    eventListener.get().accept(EventType.CHILD_CHANGED);
    assertThat(cut.shutdownAsync()).succeedsWithin(Duration.ofSeconds(2));
    assertThat(scanTimes.get()).isEqualTo(2);
  }

  @Test
  public void testScanWorkerNodesOnChildChangeOnly() throws InterruptedException {
    AtomicReference<Consumer<EventType>> eventListener = new AtomicReference<>();
    CountDownLatch countDownLatch = new CountDownLatch(2);
    AtomicInteger scanTimes = new AtomicInteger(0);
    new MockUp<AbstractCloudSchedulerObserver>() {
      @Mock
      public void masterNodeUp(UUID nodeId, Instant time) {
        countDownLatch.countDown();
      }
    };
    new MockUp<JobServiceImpl>() {
      @Mock
      public CompletableFuture<List<UUID>> getCurrentWorkersAsync(Consumer<EventType> listener) {
        eventListener.set(listener);
        countDownLatch.countDown();
        scanTimes.incrementAndGet();
        return CompletableFuture.completedFuture(Collections.emptyList());
      }
    };
    new Expectations() {
      {
        lock.lock();
        result = CompletableFuture.completedFuture(null);
        lock.unlock();
        result = CompletableFuture.completedFuture(null);

        jobService.listAllJobDefinitionsAsync((Consumer<EventType>) any);
        result = CompletableFuture.completedFuture(Collections.emptyList());
        jobService.listAllJobInstanceIdsAsync();
        result = CompletableFuture.completedFuture(Collections.emptyList());
      }
    };
    assertThat(cut.startAsync()).isDone().isCompleted();
    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(eventListener.get()).isNotNull();
    eventListener.get().accept(EventType.ENTITY_UPDATED);
    assertThat(cut.shutdownAsync()).succeedsWithin(Duration.ofSeconds(2));
    assertThat(scanTimes.get()).isEqualTo(1);
  }

  static <T> CompletableFuture<T> exceptionalCompletableFuture(Throwable exp) {
    CompletableFuture<T> future = new CompletableFuture<T>();
    future.completeExceptionally(exp);
    return future;
  }
}
