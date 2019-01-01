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

package io.github.cloudscheduler.util.retry;

import java.io.IOError;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Wei Gao
 */
public class RetryStrategyTest {
  private static final Logger logger = LoggerFactory.getLogger(RetryStrategyTest.class);

  @Test(expectedExceptions = RuntimeException.class)
  public void testFixedRetry() throws Throwable {
    AtomicInteger counter = new AtomicInteger(0);
    RetryStrategy retryer = RetryStrategy.newBuilder()
        .fixedDelay(100L)
        .maxRetry(3)
        .retryOn(Collections.singletonList(RuntimeException.class))
        .build();
    try {
      retryer.call(() ->
          CompletableFuture.completedFuture(null).thenAccept(n -> {
            counter.incrementAndGet();
            throw new RuntimeException();
          })).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    } finally {
      Assert.assertEquals(counter.get(), 3);
    }
  }

  @Test(expectedExceptions = KeeperException.SessionExpiredException.class)
  public void testStopAtAndRetryOn() throws Throwable {
    AtomicInteger counter = new AtomicInteger(0);
    RetryStrategy retryer = RetryStrategy.newBuilder()
        .fixedDelay(100L)
        .maxRetry(3)
        .retryOn(Collections.singletonList(KeeperException.class))
        .stopAt(Arrays.asList(KeeperException.NoAuthException.class,
            KeeperException.SessionExpiredException.class))
        .build();
    try {
      retryer.call(() -> {
        CompletableFuture<Void> future = new CompletableFuture<>();
        int t = counter.incrementAndGet();
        if (t == 1) {
          future.completeExceptionally(KeeperException.create(KeeperException.Code.BADVERSION));
        } else {
          future.completeExceptionally(KeeperException.create(KeeperException.Code.SESSIONEXPIRED));
        }
        return future;
      }).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    } finally {
      Assert.assertEquals(counter.get(), 2);
    }
  }

  @Test(expectedExceptions = KeeperException.BadVersionException.class)
  public void testExceedMaxRetry() throws Throwable {
    AtomicInteger counter = new AtomicInteger(0);
    RetryStrategy retryer = RetryStrategy.newBuilder()
        .fixedDelay(100L)
        .maxRetry(3)
        .retryOn(Collections.singletonList(KeeperException.class))
        .stopAt(Arrays.asList(KeeperException.NoAuthException.class,
            KeeperException.SessionExpiredException.class))
        .build();
    try {
      retryer.call(() -> {
        CompletableFuture<Void> future = new CompletableFuture<>();
        counter.incrementAndGet();
        future.completeExceptionally(KeeperException.create(KeeperException.Code.BADVERSION));
        return future;
      }).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    } finally {
      Assert.assertEquals(counter.get(), 3);
    }
  }

  @Test
  public void testExponentialNextDelay() {
    RetryStrategy retryer = RetryStrategy.newBuilder()
        .exponential(100L)
        .maxDelay(50000L)
        .build();
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 1), 100L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 2), 200L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 5), 1600L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 8), 12800L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 9), 25600L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 10), 50000L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 100), 50000L);
  }

  @Test
  public void testFibonacciNextDelay() {
    RetryStrategy retryer = RetryStrategy.newBuilder()
        .fibonacci(100L)
        .build();
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 1), 100L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 2), 100L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 3), 200L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 4), 300L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 10), 5500L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 20), 676500L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 50), 1258626902500L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 80), 2341672834846768500L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 82), 6130579072161159100L);
    Assert.assertEquals(retryer.nextRetryDelay(null, null, 100), 6130579072161159100L);
  }

  @Test
  public void testRetryOnStopAt() {
    RetryStrategy retryer = RetryStrategy.newBuilder()
        .fixedDelay(100L)
        .retryOn(Collections.singletonList(RuntimeException.class))
        .maxRetry(5)
        .build();
    Assert.assertTrue(retryer.shouldRetry(null, new RuntimeException(), 1));
    Assert.assertTrue(retryer.shouldRetry(null, new RuntimeException(), 4));
    Assert.assertFalse(retryer.shouldRetry(null, new RuntimeException(), 5));
    Assert.assertTrue(retryer.shouldRetry(null, new IllegalArgumentException(), 3));
    Assert.assertTrue(retryer.shouldRetry(null, new IllegalStateException(), 2));
    Assert.assertFalse(retryer.shouldRetry(null, new IllegalStateException(), 7));
    Assert.assertFalse(retryer.shouldRetry(null, new Exception(), 2));
    Assert.assertFalse(retryer.shouldRetry(null, new IOException(), 1));
    Assert.assertFalse(retryer.shouldRetry(null, new IOError(new RuntimeException()), 1));
    retryer = RetryStrategy.newBuilder()
        .fixedDelay(100L)
        .retryOn(Collections.singletonList(KeeperException.class))
        .stopAt(Arrays.asList(KeeperException.NoAuthException.class,
            KeeperException.SessionExpiredException.class))
        .maxRetry(5)
        .build();
    Assert.assertTrue(retryer.shouldRetry(null, KeeperException.create(KeeperException.Code.BADVERSION), 1));
    Assert.assertFalse(retryer.shouldRetry(null, KeeperException.create(KeeperException.Code.APIERROR), 5));
    Assert.assertFalse(retryer.shouldRetry(null, KeeperException.create(KeeperException.Code.NOAUTH), 1));
  }
}
