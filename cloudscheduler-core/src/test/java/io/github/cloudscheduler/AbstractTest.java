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

import io.github.cloudscheduler.service.JobService;
import io.github.cloudscheduler.service.JobServiceImpl;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Wei Gao */
public abstract class AbstractTest {
  private static final Logger logger = LoggerFactory.getLogger(AbstractTest.class);

  private static final RandomString gen = new RandomString(8, new SecureRandom());

  private static TestingServer zkTestServer;
  protected ZooKeeper zooKeeper;
  protected JobService jobService;
  protected static ExecutorService threadPool;
  protected String zkUrl;

  @BeforeAll
  public static void setup() throws Exception {
    AtomicInteger threadCounter = new AtomicInteger(0);
    threadPool =
        Executors.newCachedThreadPool(
            r -> new Thread(r, "TestThread-" + threadCounter.incrementAndGet()));
    logger.info("Starting zookeeper");
    zkTestServer = new TestingServer();
  }

  @AfterAll
  public static void teardown() {
    logger.info("Stop zookeeper server");
    try {
      zkTestServer.close();
    } catch (Throwable e) {
      logger.trace("Error when stop zookeeper server.", e);
    }
    if (threadPool != null) {
      threadPool.shutdown();
    }
  }

  @BeforeEach
  public void init() throws Exception {
    zkUrl = prepareZooKeeper();
    zooKeeper = ZooKeeperUtils.connectToZooKeeper(zkUrl, Integer.MAX_VALUE).get();
    logger.trace("Zookeeper for testing: {}", zooKeeper);
    jobService = new JobServiceImpl(zooKeeper);
  }

  @AfterEach
  public void destroy() {
    logger.info("Close zookeeper {}", zooKeeper);
    try {
      zooKeeper.close();
    } catch (Throwable e) {
      logger.trace("Error when close zookeeper", e);
    }
  }

  public static int randomPort() {
    int min = 20480;
    int max = 40960;
    Random random = new SecureRandom();
    return min + random.nextInt(max - min);
  }

  private String prepareZooKeeper() throws ExecutionException, InterruptedException {
    String root = "/" + gen.nextString();

    ZooKeeperUtils.connectToZooKeeper(zkTestServer.getConnectString(), Integer.MAX_VALUE)
        .thenAccept(
            zk ->
                ZooKeeperUtils.createPersistentZnode(zk, root, null)
                    .whenComplete(
                        (v, cause) -> {
                          try {
                            zk.close();
                          } catch (InterruptedException e) {
                            logger.warn("Error happened when close zookeeper", e);
                          }
                        }))
        .get();
    return zkTestServer.getConnectString() + root;
  }

  static class RandomString {

    /** Generate a random string. */
    String nextString() {
      for (int idx = 0; idx < buf.length; ++idx) buf[idx] = symbols[random.nextInt(symbols.length)];
      return new String(buf);
    }

    public static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static final String lower = upper.toLowerCase(Locale.ROOT);

    public static final String digits = "0123456789";

    public static final String alphanum = upper + lower + digits;

    private final Random random;

    private final char[] symbols;

    private final char[] buf;

    public RandomString(int length, Random random, String symbols) {
      if (length < 1) throw new IllegalArgumentException();
      if (symbols.length() < 2) throw new IllegalArgumentException();
      this.random = Objects.requireNonNull(random);
      this.symbols = symbols.toCharArray();
      this.buf = new char[length];
    }

    /** Create an alphanumeric string generator. */
    public RandomString(int length, Random random) {
      this(length, random, alphanum);
    }

    /** Create an alphanumeric strings from a secure generator. */
    public RandomString(int length) {
      this(length, new SecureRandom());
    }

    /** Create session identifiers. */
    public RandomString() {
      this(21);
    }
  }
}
