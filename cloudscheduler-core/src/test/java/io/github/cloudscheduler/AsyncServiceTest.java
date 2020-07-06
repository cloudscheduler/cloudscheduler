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

package io.github.cloudscheduler;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import mockit.Mocked;
import mockit.Tested;
import org.junit.jupiter.api.Test;

public class AsyncServiceTest {
  class TestAsyncService implements AsyncService {

    @Override
    public CompletableFuture<Void> startAsync() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> shutdownAsync() {
      return CompletableFuture.completedFuture(null);
    }
  }

  @Tested private AsyncService cut = new TestAsyncService();

  @Test
  public void testStart() {
    assertThatCode(cut::start).doesNotThrowAnyException();
  }

  @Test
  public void testShutdown() {
    assertThatCode(cut::shutdown).doesNotThrowAnyException();
  }

  @Test
  public void testStartRuntimeException(@Mocked TestAsyncService service) {
    AsyncService testService =
        new AsyncService() {
          @Override
          public CompletableFuture<Void> startAsync() {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException());
            return future;
          }

          @Override
          public CompletableFuture<Void> shutdownAsync() {
            return null;
          }
        };
    assertThatExceptionOfType(RuntimeException.class).isThrownBy(testService::start);
  }

  @Test
  public void testStartCheckedException(@Mocked TestAsyncService service) {
    AsyncService testService =
        new AsyncService() {
          @Override
          public CompletableFuture<Void> startAsync() {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IOException());
            return future;
          }

          @Override
          public CompletableFuture<Void> shutdownAsync() {
            return null;
          }
        };
    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(testService::start)
        .havingCause()
        .isInstanceOf(IOException.class);
  }
}
