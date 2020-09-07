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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOError;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

/** @author Wei Gao */
public class RetryStrategyTest {
  @Test
  public void testFixedRetry() {
    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(
            () -> {
              AtomicInteger counter = new AtomicInteger(0);
              RetryStrategy retryer =
                  RetryStrategy.newBuilder()
                      .fixedDelay(100L)
                      .maxRetry(3)
                      .retryOn(Collections.singletonList(RuntimeException.class))
                      .build();
              try {
                retryer
                    .call(
                        () ->
                            CompletableFuture.completedFuture(null)
                                .thenAccept(
                                    n -> {
                                      counter.incrementAndGet();
                                      throw new RuntimeException();
                                    }))
                    .get();
              } catch (ExecutionException e) {
                throw e.getCause();
              } finally {
                assertThat(counter.get()).isEqualTo(3);
              }
            });
  }

  @Test
  public void testStopAtAndRetryOn() {
    assertThatExceptionOfType(KeeperException.SessionExpiredException.class)
        .isThrownBy(
            () -> {
              AtomicInteger counter = new AtomicInteger(0);
              RetryStrategy retryer =
                  RetryStrategy.newBuilder()
                      .fixedDelay(100L)
                      .maxRetry(3)
                      .retryOn(Collections.singletonList(KeeperException.class))
                      .stopAt(
                          Arrays.asList(
                              KeeperException.NoAuthException.class,
                              KeeperException.SessionExpiredException.class))
                      .build();
              try {
                retryer
                    .call(
                        () -> {
                          CompletableFuture<Void> future = new CompletableFuture<>();
                          int t = counter.incrementAndGet();
                          if (t == 1) {
                            future.completeExceptionally(
                                KeeperException.create(KeeperException.Code.BADVERSION));
                          } else {
                            future.completeExceptionally(
                                KeeperException.create(KeeperException.Code.SESSIONEXPIRED));
                          }
                          return future;
                        })
                    .get();
              } catch (ExecutionException e) {
                throw e.getCause();
              } finally {
                assertThat(counter.get()).isEqualTo(2);
              }
            });
  }

  @Test
  public void testExceedMaxRetry() {
    assertThatExceptionOfType(KeeperException.BadVersionException.class)
        .isThrownBy(
            () -> {
              AtomicInteger counter = new AtomicInteger(0);
              RetryStrategy retryer =
                  RetryStrategy.newBuilder()
                      .fixedDelay(100L)
                      .maxRetry(3)
                      .retryOn(Collections.singletonList(KeeperException.class))
                      .stopAt(
                          Arrays.asList(
                              KeeperException.NoAuthException.class,
                              KeeperException.SessionExpiredException.class))
                      .build();
              try {
                retryer
                    .call(
                        () -> {
                          CompletableFuture<Void> future = new CompletableFuture<>();
                          counter.incrementAndGet();
                          future.completeExceptionally(
                              KeeperException.create(KeeperException.Code.BADVERSION));
                          return future;
                        })
                    .get();
              } catch (ExecutionException e) {
                throw e.getCause();
              } finally {
                assertThat(counter.get()).isEqualTo(3);
              }
            });
  }

  @Test
  public void testIncrementalNextDelay() {
    RetryStrategy retryer =
        RetryStrategy.newBuilder().incrementDelay(100L, 100L).maxDelay(50000L).build();
    assertThat(retryer.nextRetryDelay(null, null, 1)).isEqualTo(200L);
    assertThat(retryer.nextRetryDelay(null, null, 2)).isEqualTo(300L);
    assertThat(retryer.nextRetryDelay(null, null, 5)).isEqualTo(600L);
    assertThat(retryer.nextRetryDelay(null, null, 8)).isEqualTo(900L);
    assertThat(retryer.nextRetryDelay(null, null, 9)).isEqualTo(1000L);
    assertThat(retryer.nextRetryDelay(null, null, 10)).isEqualTo(1100L);
    assertThat(retryer.nextRetryDelay(null, null, 100)).isEqualTo(10100L);
  }

  @Test
  public void testExponentialNextDelay() {
    RetryStrategy retryer = RetryStrategy.newBuilder().exponential(100L).maxDelay(50000L).build();
    assertThat(retryer.nextRetryDelay(null, null, 1)).isEqualTo(100L);
    assertThat(retryer.nextRetryDelay(null, null, 2)).isEqualTo(200L);
    assertThat(retryer.nextRetryDelay(null, null, 5)).isEqualTo(1600L);
    assertThat(retryer.nextRetryDelay(null, null, 8)).isEqualTo(12800L);
    assertThat(retryer.nextRetryDelay(null, null, 9)).isEqualTo(25600L);
    assertThat(retryer.nextRetryDelay(null, null, 10)).isEqualTo(50000L);
    assertThat(retryer.nextRetryDelay(null, null, 100)).isEqualTo(50000L);
  }

  @Test
  public void testFibonacciNextDelay() {
    RetryStrategy retryer = RetryStrategy.newBuilder().fibonacci(100L).build();
    assertThat(retryer.nextRetryDelay(null, null, 1)).isEqualTo(100L);
    assertThat(retryer.nextRetryDelay(null, null, 2)).isEqualTo(100L);
    assertThat(retryer.nextRetryDelay(null, null, 3)).isEqualTo(200L);
    assertThat(retryer.nextRetryDelay(null, null, 4)).isEqualTo(300L);
    assertThat(retryer.nextRetryDelay(null, null, 10)).isEqualTo(5500L);
    assertThat(retryer.nextRetryDelay(null, null, 20)).isEqualTo(676500L);
    assertThat(retryer.nextRetryDelay(null, null, 50)).isEqualTo(1258626902500L);
    assertThat(retryer.nextRetryDelay(null, null, 80)).isEqualTo(2341672834846768500L);
    assertThat(retryer.nextRetryDelay(null, null, 82)).isEqualTo(6130579072161159100L);
    assertThat(retryer.nextRetryDelay(null, null, 100)).isEqualTo(6130579072161159100L);
  }

  @Test
  public void testRetryOnStopAt() {
    RetryStrategy retryer =
        RetryStrategy.newBuilder()
            .fixedDelay(100L)
            .retryOn(Collections.singletonList(RuntimeException.class))
            .maxRetry(5)
            .build();
    assertThat(retryer.shouldRetry(new RuntimeException(), 1)).isTrue();
    assertThat(retryer.shouldRetry(new RuntimeException(), 4)).isTrue();
    assertThat(retryer.shouldRetry(new RuntimeException(), 5)).isFalse();
    assertThat(retryer.shouldRetry(new IllegalArgumentException(), 3)).isTrue();
    assertThat(retryer.shouldRetry(new IllegalStateException(), 2)).isTrue();
    assertThat(retryer.shouldRetry(new IllegalStateException(), 7)).isFalse();
    assertThat(retryer.shouldRetry(new Exception(), 2)).isFalse();
    assertThat(retryer.shouldRetry(new IOException(), 1)).isFalse();
    assertThat(retryer.shouldRetry(new IOError(new RuntimeException()), 1)).isFalse();
    retryer =
        RetryStrategy.newBuilder()
            .fixedDelay(100L)
            .retryOn(Collections.singletonList(KeeperException.class))
            .stopAt(
                Arrays.asList(
                    KeeperException.NoAuthException.class,
                    KeeperException.SessionExpiredException.class))
            .maxRetry(5)
            .build();
    assertThat(retryer.shouldRetry(KeeperException.create(KeeperException.Code.BADVERSION), 1))
        .isTrue();
    assertThat(retryer.shouldRetry(KeeperException.create(KeeperException.Code.APIERROR), 5))
        .isFalse();
    assertThat(retryer.shouldRetry(KeeperException.create(KeeperException.Code.NOAUTH), 1))
        .isFalse();
  }
}
