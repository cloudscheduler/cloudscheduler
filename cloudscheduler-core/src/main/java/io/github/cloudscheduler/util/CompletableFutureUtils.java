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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Completable future helpers.
 *
 * @author Wei Gao
 */
public abstract class CompletableFutureUtils {
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
      r -> new Thread(r, "CompletableFutureDelayScheduler"));

  /**
   * Return a completable future with exception.
   *
   * @param cause exception cause
   * @param <T> data type
   * @return completable future with exception
   */
  public static <T> CompletableFuture<T> exceptionalCompletableFuture(Throwable cause) {
    CompletableFuture<T> exceptionFuture = new CompletableFuture<>();
    exceptionFuture.completeExceptionally(cause);
    return exceptionFuture;
  }

  /**
   * Return a completable future that will complete after timeout.
   *
   * @param timeout timeout value
   * @param value complete value
   * @param <T> data type
   * @return completable future
   */
  public static <T> CompletableFuture<T> completeAfter(Duration timeout, T value) {
    CompletableFuture<T> result = new CompletableFuture<>();
    scheduler.schedule(() -> result.complete(value), timeout.toMillis(), TimeUnit.MILLISECONDS);
    return result;
  }

  /**
   * Return a timeout completable future.
   *
   * @param timeout timeout value
   * @param <T> data type
   * @return completable future
   */
  public static <T> CompletableFuture<T> timeoutAfter(Duration timeout) {
    TimeoutCompletableFuture<T> result = new TimeoutCompletableFuture<>();
    result.setFuture(scheduler.schedule(() ->
            result.completeExceptionally(new TimeoutException("timeout")),
        timeout.toMillis(), TimeUnit.MILLISECONDS));
    return result;
  }

  private static class TimeoutCompletableFuture<T> extends CompletableFuture<T> {
    private ScheduledFuture<?> future;

    void setFuture(ScheduledFuture<?> future) {
      this.future = future;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      future.cancel(mayInterruptIfRunning);
      return super.cancel(mayInterruptIfRunning);
    }
  }
}
