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

package io.github.cloudscheduler.util;

import io.github.cloudscheduler.codec.EntityCodec;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * @author Wei Gao
 */
public class ZooKeeperUtilsTest {
  private final static Logger logger = LoggerFactory.getLogger(ZooKeeperUtilsTest.class);

  private TestingServer zkTestServer;
  private ExecutorService threadPool;
  private ZooKeeper zooKeeper;
  private EntityCodec<byte[]> codec = new EntityCodec<byte[]>() {
    @Override
    public byte[] encode(byte[] entity) {
      return entity;
    }

    @Override
    public byte[] decode(byte[] data) {
      return data;
    }
  };
  private String path = "/test";

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
    zooKeeper = ZooKeeperUtils.connectToZooKeeper(zkTestServer.getConnectString(), Integer.MAX_VALUE).get();
    ZooKeeperUtils.createZnode(zooKeeper, path, CreateMode.PERSISTENT, new byte[] {0x01, 0x02, 0x03, 0x04, 0x05}).get();
  }

  private void closeZooKeeper(ZooKeeper zkClient, CountDownLatch countDownLatch) {
    try {
      if (zkClient != null) {
        logger.trace("Close zookeeper.");
        zkClient.close();
      }
    } catch (Throwable e) {
      logger.info("Close zookeeper throw exception, ignore it.", e);
    } finally {
      countDownLatch.countDown();
    }
  }

  @AfterClass
  public void destroy() throws Exception {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    ZooKeeperUtils.deleteIfExists(zooKeeper, path, true).whenComplete((v, cause) -> {
      try {
        zooKeeper.close();
      } catch (Throwable e) {
        logger.trace("Error when close zookeeper", e);
      }
      logger.info("Stop zookeeper");
      try {
        zkTestServer.close();
      } catch (Throwable e) {
        logger.trace("Error when close zookeeper server.", e);
      }
      threadPool.shutdown();
      countDownLatch.countDown();
    });
    countDownLatch.await();
  }
}
