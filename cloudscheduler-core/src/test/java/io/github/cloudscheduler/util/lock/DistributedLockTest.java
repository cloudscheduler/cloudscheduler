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

package io.github.cloudscheduler.util.lock;

import io.github.cloudscheduler.util.ZooKeeperUtils;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Wei Gao
 */
public class DistributedLockTest {
  private final static Logger logger = LoggerFactory.getLogger(DistributedLockTest.class);
  private final static String LOCK_FOLDER = "/locks";

  private TestingServer zkTestServer;
  private ExecutorService threadPool;

  @BeforeClass
  public void init() throws Exception {
    logger.info("Starting zookeeper");
    AtomicInteger threadCounter = new AtomicInteger(0);
    threadPool = Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r);
      t.setName("worker-" + threadCounter.incrementAndGet());
      return t;
    });
    zkTestServer = new TestingServer();
  }

  @Test
  public void testBasicLock() throws InterruptedException {
    String zkUrl = zkTestServer.getConnectString();
    UUID id = UUID.randomUUID();
    AtomicBoolean worked = new AtomicBoolean(false);
    doWithLock(id, zkUrl, "testLock", () -> {
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException e) {
        logger.warn("Interrupted", e);
      }
      worked.set(true);
    });
    logger.info("Check if it's acquired.");
    Assert.assertTrue(worked.get(), "Lock never acquired.");
  }

  @Test
  public void testLockWithName() throws InterruptedException {
    String zkUrl = zkTestServer.getConnectString();
    UUID id = UUID.randomUUID();
    AtomicBoolean worked = new AtomicBoolean(false);
    doWithLock(id, zkUrl, null, () -> worked.set(true));
    Assert.assertTrue(worked.get(), "Lock never acquired.");
  }

  @Test
  public void testTwoNodeLock() throws ExecutionException, InterruptedException {
    String zkUrl = zkTestServer.getConnectString();

    String lockName = "testLock";

    UUID id1 = UUID.randomUUID();
    UUID id2 = UUID.randomUUID();
    AtomicBoolean l1Locked = new AtomicBoolean(false);
    AtomicBoolean l2Locked = new AtomicBoolean(false);

    AtomicBoolean l1InLock = new AtomicBoolean(false);
    AtomicBoolean l2InLock = new AtomicBoolean(false);
    Runnable r1 = () -> {
      try {
        doWithLock(id1, zkUrl, lockName, () -> {
          l1Locked.set(true);
          synchronized (l1InLock) {
            l1InLock.set(true);
            l1InLock.notifyAll();
          }
          try {
            Thread.sleep(500L);
          } catch (InterruptedException e) {
            logger.trace("Ignore it.", e);
          }
          logger.trace("Release lock");
        });
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };
    Runnable r2 = () -> {
      try {
        synchronized (l1InLock) {
          if (!l1InLock.get()) {
            l1InLock.wait();
          }
        }
        doWithLock(id2, zkUrl, lockName, () -> {
          l2Locked.set(true);
          synchronized (l2InLock) {
            l2InLock.set(true);
            l2InLock.notifyAll();
          }
        });
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };
    Future<?> f1 = threadPool.submit(r1);
    Future<?> f2 = threadPool.submit(r2);
    f1.get();
    f2.get();
    Assert.assertTrue(l1Locked.get(), "Lock 1 never acquired lock");
    Assert.assertTrue(l2Locked.get(), "Lock 2 never acquired lock");
  }

  @Test
  public void testLockTwiceFromSameNode() throws InterruptedException {
    String zkUrl = zkTestServer.getConnectString();
    UUID id = UUID.randomUUID();
    String lockName = "testLock";

    AtomicBoolean l1Lock = new AtomicBoolean(false);
    AtomicBoolean l2Lock = new AtomicBoolean(false);

    doWithLock(id, zkUrl, lockName, () -> {
      l1Lock.set(true);
      try {
        doWithLock(id, zkUrl, lockName, () -> l2Lock.set(true));
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    });
    Assert.assertTrue(l1Lock.get(), "Level 1 lock not acquired");
    Assert.assertTrue(l2Lock.get(), "Level 2 lock not acquired");
  }

  @Test
  public void testUnlockBeforeAcquired() throws ExecutionException, InterruptedException {
    String zkUrl = zkTestServer.getConnectString();
    UUID id1 = UUID.randomUUID();
    UUID id2 = UUID.randomUUID();
    String lockName = "testLock";

    CountDownLatch countDownLatch1 = new CountDownLatch(1);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    CountDownLatch countDownLatch3 = new CountDownLatch(1);

    AtomicBoolean l1 = new AtomicBoolean(false);
    AtomicBoolean l2 = new AtomicBoolean(false);

    Lock lock = new ReentrantLock();
    Condition cond = lock.newCondition();

    ZooKeeper zk1 = ZooKeeperUtils.connectToZooKeeper(zkUrl, Integer.MAX_VALUE).get();
    DistributedLock lock1 = new DistributedLockImpl(id1, zk1, LOCK_FOLDER, lockName);

    ZooKeeper zk2 = ZooKeeperUtils.connectToZooKeeper(zkUrl, Integer.MAX_VALUE).get();
    DistributedLock lock2 = new DistributedLockImpl(id2, zk2, LOCK_FOLDER, lockName);

    lock1.lock().thenAccept(v -> {
      logger.trace("Lock1 acquired lock");
      l1.set(true);
      countDownLatch1.countDown();
      lock.lock();
      try {
        cond.await();
      } catch (InterruptedException e) {
        logger.error("Error happened when wait for lock2 done.");
      } finally {
        lock.unlock();
      }
    }).exceptionally(cause -> {
      logger.error("Error hapened in lock1", cause);
      return null;
    })
        .thenCompose(v -> lock1.unlock())
        .exceptionally(cause -> {
          logger.error("Error happened when unlock lock1", cause);
          return null;
        }).whenComplete((v, cause) -> countDownLatch2.countDown());

    countDownLatch1.await();

    lock2.lock().thenAccept(v -> {
      logger.trace("Lock2 acquired lock");
      l2.set(true);
    });
    Thread.sleep(1000L);
    lock2.unlock().exceptionally(cause -> {
      logger.error("Error happened in lock2 unlock", cause);
      return null;
    }).thenAccept(v -> {
      logger.trace("Lock2 unlocked");
      lock.lock();
      try {
        cond.signal();
      } finally {
        lock.unlock();
      }
    }).whenComplete((v, cause) -> countDownLatch3.countDown());

    Thread.sleep(1000L);

    countDownLatch2.await();
    countDownLatch3.await();
    zk1.close();
    zk2.close();
  }

  private void doWithLock(UUID id, String zkUrl, String locker, Runnable runnable)
      throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    ZooKeeperUtils.connectToZooKeeper(zkUrl, Integer.MAX_VALUE).thenCompose(zooKeeper -> {
      try {
        DistributedLock lock = new DistributedLockImpl(id, zooKeeper, LOCK_FOLDER, locker);
        return lock.lock().thenAccept(v -> {
          try {
            logger.info("{}: Lock acquired", locker);
            runnable.run();
          } catch (Throwable e) {
            logger.warn("Error happened", e);
          }
        }).thenCompose(v -> lock.unlock())
            .whenComplete((v, cause) -> {
              try {
                logger.trace("Close zookeeper.");
                zooKeeper.close();
              } catch (Throwable e) {
                logger.info("Close zookeeper throw exception, ignore it.", e);
              } finally {
                countDownLatch.countDown();
              }
            });
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    });
    countDownLatch.await();
  }

  @AfterClass
  public void destroy() throws IOException {
    logger.info("Stop zookeeper");
    zkTestServer.close();
    threadPool.shutdown();
  }
}
