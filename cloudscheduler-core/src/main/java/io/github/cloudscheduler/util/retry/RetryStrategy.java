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

import io.github.cloudscheduler.util.CompletableFutureUtils;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retry strategy. Using completable future to implement retry logic.
 *
 * @author Wei Gao
 */
public abstract class RetryStrategy {

  private static final Random random = new SecureRandom();
  private static final Logger logger = LoggerFactory.getLogger(RetryStrategy.class);
  protected final long maxDelay;
  private final int maxRetryTimes;
  private final boolean retryOnNull;
  private final List<Class<? extends Throwable>> stopAt;
  private final List<Class<? extends Throwable>> retryOn;
  private final boolean useRandom;

  RetryStrategy(int maxRetryTimes, long maxDelay, List<Class<? extends Throwable>> retryOn,
                List<Class<? extends Throwable>> stopAt, boolean useRandom, boolean retryOnNull) {
    this.maxRetryTimes = maxRetryTimes;
    this.maxDelay = maxDelay;
    this.retryOn = retryOn;
    this.stopAt = stopAt;
    this.useRandom = useRandom;
    this.retryOnNull = retryOnNull;
  }

  /**
   * Use to call logic with retry.
   *
   * @param operation operation
   * @param <T>       data type
   * @return completable future
   */
  public <T> CompletableFuture<T> call(Supplier<CompletableFuture<T>> operation) {
    CompletableFuture<T> result = new CompletableFuture<>();
    AtomicReference<Consumer<Integer>> consumerHolder = new AtomicReference<>();
    try {
      consumerHolder.set((retries) -> {
        logger.trace("{} try", retries);
        operation.get().whenComplete((v, exp) -> {
          if (exp == null && (!retryOnNull || v != null)) {
            logger.trace("Operation success, complete value result");
            result.complete(v);
          } else {
            Throwable cause = exp;
            if (exp != null) {
              if (exp instanceof CompletionException || exp instanceof ExecutionException) {
                cause = exp.getCause();
              }
            }
            if (shouldRetry(v, cause, retries)) {
              logger.trace("Will retry");
              long interval = nextRetryDelay(v, cause, retries);
              if (interval <= 0L) {
                logger.trace("Retry now");
                consumerHolder.get().accept(retries + 1);
              } else {
                logger.trace("Scheduling retry");
                CompletableFutureUtils.completeAfter(Duration.ofMillis(interval), null)
                    .thenAccept(n -> consumerHolder.get().accept(retries + 1));
              }
            } else {
              logger.trace("Shouldn't retry");
              if (cause != null) {
                logger.trace("Complete with exception");
                result.completeExceptionally(cause);
              } else {
                logger.trace("Complete with value");
                result.complete(v);
              }
            }
          }
        });
      });
    } catch (Throwable e) {
      result.completeExceptionally(e);
    }
    consumerHolder.get().accept(1);
    return result;
  }

  <T> long nextRetryDelay(T v, Throwable exp, int retries) {
    long delay = nextRetryInterval(v, exp, retries);
    if (useRandom && delay > 0L) {
      delay = (random.nextLong() % delay);
    }
    return delay;
  }

  protected abstract <T> long nextRetryInterval(T v, Throwable exp, int retries);

  protected <T> boolean shouldRetry(T v, Throwable exp, int retries) {
    if (!retryOn.isEmpty() && (maxRetryTimes <= 0 || retries < maxRetryTimes)) {
      boolean retry = false;
      for (Class<? extends Throwable> clazz : retryOn) {
        if (clazz.isInstance(exp)) {
          retry = true;
          break;
        }
      }
      if (retry) {
        for (Class<? extends Throwable> clazz : stopAt) {
          if (clazz.isInstance(exp)) {
            retry = false;
            break;
          }
        }
      }
      return retry;
    }
    return false;
  }

  private enum Type {
    FIXED_DELAY, INCREMENTAL_DELAY, EXPONENTIAL, FIBONACCI
  }

  private static final class ExponentialRetryStrategy extends RetryStrategy {
    private final long multiplier;

    ExponentialRetryStrategy(long multiplier, int maxRetryTimes, long maxDelay,
                             List<Class<? extends Throwable>> retryOn,
                             List<Class<? extends Throwable>> stopAt,
                             boolean useRandom, boolean retryOnNull) {
      super(maxRetryTimes, maxDelay, retryOn, stopAt, useRandom, retryOnNull);
      this.multiplier = multiplier;
    }


    @Override
    protected <T> long nextRetryInterval(T v, Throwable exp, int retries) {
      int t = 1;
      long delay = multiplier;
      while (t < retries && (maxDelay <= 0L || delay < maxDelay)) {
        long a = delay << 1;
        if (a > 0L) {
          delay = a;
        } else {
          break;
        }
        t++;
      }
      if (maxDelay > 0L && delay > maxDelay) {
        delay = maxDelay;
      }
      return delay;
    }
  }

  private static final class FibonacciRetryStrategy extends RetryStrategy {
    private final long multiplier;

    FibonacciRetryStrategy(long multiplier, int maxRetryTimes, long maxDelay,
                           List<Class<? extends Throwable>> retryOn,
                           List<Class<? extends Throwable>> stopAt,
                           boolean useRandom, boolean retryOnNull) {
      super(maxRetryTimes, maxDelay, retryOn, stopAt, useRandom, retryOnNull);
      this.multiplier = multiplier;
    }

    @Override
    protected <T> long nextRetryInterval(T v, Throwable exp, int retries) {
      int t = 1;
      long delay = multiplier;
      long n;
      long a = 0L;
      while (t < retries && (maxDelay <= 0L || delay < maxDelay)) {
        n = a + delay;
        if (n > 0L) {
          a = delay;
          delay = n;
        } else {
          break;
        }
        t++;
      }
      if (maxDelay > 0L && delay > maxDelay) {
        delay = maxDelay;
      }
      return delay;
    }
  }

  private static final class FixedDelayRetryStrategy extends RetryStrategy {
    private final long delay;

    FixedDelayRetryStrategy(long delay, int maxRetryTimes, List<Class<? extends Throwable>> retryOn,
                            List<Class<? extends Throwable>> stopAt, boolean useRandom,
                            boolean retryOnNull) {
      super(maxRetryTimes, -1L, retryOn, stopAt, useRandom, retryOnNull);
      this.delay = delay;
    }

    @Override
    protected <T> long nextRetryInterval(T v, Throwable exp, int retries) {
      return delay;
    }
  }

  private static final class IncrementalDelayRetryStrategy extends RetryStrategy {
    private final long initialDelay;
    private final long increment;

    IncrementalDelayRetryStrategy(long initialDelay, long increment, int maxRetryTimes,
                                  long maxDelay, List<Class<? extends Throwable>> retryOn,
                                  List<Class<? extends Throwable>> stopAt,
                                  boolean useRandom, boolean retryOnNull) {
      super(maxRetryTimes, maxDelay, retryOn, stopAt, useRandom, retryOnNull);
      this.initialDelay = initialDelay;
      this.increment = increment;
    }

    @Override
    protected <T> long nextRetryInterval(T v, Throwable exp, int retries) {
      long delay = initialDelay;
      int t = 0;
      while (t < retries && (maxDelay <= 0L || delay < maxDelay)) {
        long a = delay + increment;
        if (a > 0L) {
          delay = a;
        } else {
          break;
        }
        t++;
      }
      if (maxDelay > 0L && delay > maxDelay) {
        delay = maxDelay;
      }
      return delay;
    }
  }

  /**
   * Create a new instance of builder.
   *
   * @return builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * RetryStrategy builder.
   */
  public static final class Builder {
    private Type type;
    private long initialDelay;
    private long delay;
    private int maxRetry;
    private long multiplier;
    private long maxDelay;
    private final List<Class<? extends Throwable>> retryOn;
    private final List<Class<? extends Throwable>> stopAt;
    private boolean retryOnNull;
    private boolean useRandom;

    private Builder() {
      type = Type.FIXED_DELAY;
      retryOn = new ArrayList<>();
      stopAt = new ArrayList<>();
    }

    /**
     * Retry with fixed delay.
     *
     * @param delay delay
     * @return builder
     */
    public Builder fixedDelay(long delay) {
      type = Type.FIXED_DELAY;
      this.delay = delay;
      return this;
    }

    /**
     * Retry with increment delay.
     *
     * @param initialDelay initial delay
     * @param delay        increment of delay
     * @return builder
     */
    public Builder incrementDelay(long initialDelay, long delay) {
      type = Type.INCREMENTAL_DELAY;
      this.initialDelay = initialDelay;
      this.delay = delay;
      return this;
    }

    /**
     * Retry with exponential delay.
     *
     * @param multiplier exponential multiplier
     * @return builder
     */
    public Builder exponential(long multiplier) {
      type = Type.EXPONENTIAL;
      this.multiplier = multiplier;
      return this;
    }

    /**
     * Retry with fibonacci delay.
     *
     * @param multiplier fibonacci multiplier
     * @return builder
     */
    public Builder fibonacci(long multiplier) {
      type = Type.FIBONACCI;
      this.multiplier = multiplier;
      return this;
    }

    /**
     * Maximum retry times.
     *
     * @param maxRetry maximum retry
     * @return builder
     */
    public Builder maxRetry(int maxRetry) {
      this.maxRetry = maxRetry;
      return this;
    }

    /**
     * Maximum delay.
     *
     * @param maxDelay maximum delay
     * @return builder
     */
    public Builder maxDelay(long maxDelay) {
      this.maxDelay = maxDelay;
      return this;
    }

    /**
     * Retry on exceptions.
     *
     * @param clazz exception classes
     * @return builder
     */
    public Builder retryOn(List<Class<? extends Throwable>> clazz) {
      retryOn.addAll(clazz);
      return this;
    }

    /**
     * Stop retry on exceptions.
     *
     * @param clazz stop retry exception classes
     * @return builder
     */
    public Builder stopAt(List<Class<? extends Throwable>> clazz) {
      stopAt.addAll(clazz);
      return this;
    }

    /**
     * Randomize delay.
     *
     * @return builder
     */
    public Builder random() {
      return random(true);
    }

    /**
     * If randomize delay.
     *
     * @param random random
     * @return builder
     */
    public Builder random(boolean random) {
      this.useRandom = random;
      return this;
    }

    /**
     * Retry if result is null.
     *
     * @return builder
     */
    public Builder retryOnNull() {
      return retryOnNull(true);
    }

    /**
     * Set if retry if result is null.
     *
     * @param retryOnNull retry if null
     * @return builder
     */
    public Builder retryOnNull(boolean retryOnNull) {
      this.retryOnNull = retryOnNull;
      return this;
    }

    /**
     * Builder retry strategy.
     *
     * @return retry strategy instance
     */
    public RetryStrategy build() {
      switch (type) {
        case FIXED_DELAY:
          return new FixedDelayRetryStrategy(delay, maxRetry, retryOn, stopAt, useRandom,
              retryOnNull);
        case INCREMENTAL_DELAY:
          return new IncrementalDelayRetryStrategy(initialDelay, delay, maxRetry, maxDelay,
              retryOn, stopAt, useRandom, retryOnNull);
        case EXPONENTIAL:
          return new ExponentialRetryStrategy(multiplier, maxRetry, maxDelay, retryOn,
              stopAt, useRandom, retryOnNull);
        case FIBONACCI:
          return new FibonacciRetryStrategy(multiplier, maxRetry, maxDelay, retryOn,
              stopAt, useRandom, retryOnNull);
        default:
          throw new IllegalStateException("Unknown retry strategy type.");
      }
    }
  }
}
