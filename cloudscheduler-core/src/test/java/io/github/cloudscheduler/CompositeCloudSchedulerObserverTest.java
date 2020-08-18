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

import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CompositeCloudSchedulerObserverTest {
  @Test
  void testMasterNodeUp() throws InterruptedException {
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void masterNodeUp(UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    super.masterNodeUp(nodeId, time);
                  }
                }));
    cut.masterNodeUp(nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testMasterNodeUpException() throws InterruptedException {
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void masterNodeUp(UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.masterNodeUp(nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testMasterNodeDown() throws InterruptedException {
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void masterNodeDown(UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    super.masterNodeDown(nodeId, time);
                  }
                }));
    cut.masterNodeDown(nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testMasterNodeDownException() throws InterruptedException {
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void masterNodeDown(UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.masterNodeDown(nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testWorkerNodeUp() throws InterruptedException {
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void workerNodeUp(UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    super.workerNodeUp(nodeId, time);
                  }
                }));
    cut.workerNodeUp(nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testWorkerNodeUpException() throws InterruptedException {
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void workerNodeUp(UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.workerNodeUp(nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testWorkerNodeDown() throws InterruptedException {
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void workerNodeDown(UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    super.workerNodeDown(nodeId, time);
                  }
                }));
    cut.workerNodeDown(nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testWorkerNodeDownException() throws InterruptedException {
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void workerNodeDown(UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.workerNodeDown(nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testWorkerNodeRemoved() throws InterruptedException {
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void workerNodeRemoved(UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    super.workerNodeRemoved(nodeId, time);
                  }
                }));
    cut.workerNodeRemoved(nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testWorkerNodeRemovedException() throws InterruptedException {
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void workerNodeRemoved(UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.workerNodeRemoved(nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobDefinitionPaused() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobDefinitionPaused(UUID jobDefId, Instant time) {
                    countDownLatch.countDown();
                    super.jobDefinitionPaused(jobDefId, time);
                  }
                }));
    cut.jobDefinitionPaused(jobDefId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobDefinitionPausedException() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobDefinitionPaused(UUID jobDefId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.jobDefinitionPaused(jobDefId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobDefinitionResumed() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobDefinitionResumed(UUID jobDefId, Instant time) {
                    countDownLatch.countDown();
                    super.jobDefinitionResumed(jobDefId, time);
                  }
                }));
    cut.jobDefinitionResumed(jobDefId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobDefinitionResumedException() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobDefinitionResumed(UUID jobDefId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.jobDefinitionResumed(jobDefId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobDefinitionCompleted() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobDefinitionCompleted(UUID jobDefId, Instant time) {
                    countDownLatch.countDown();
                    super.jobDefinitionCompleted(jobDefId, time);
                  }
                }));
    cut.jobDefinitionCompleted(jobDefId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobDefinitionCompletedException() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobDefinitionCompleted(UUID jobDefId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.jobDefinitionCompleted(jobDefId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobDefinitionRemoved() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobDefinitionRemoved(UUID jobDefId, Instant time) {
                    countDownLatch.countDown();
                    super.jobDefinitionRemoved(jobDefId, time);
                  }
                }));
    cut.jobDefinitionRemoved(jobDefId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobDefinitionRemovedException() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobDefinitionRemoved(UUID jobDefId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.jobDefinitionRemoved(jobDefId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobInstanceScheduled() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobInstanceScheduled(UUID jobDefId, UUID jobInsId, Instant time) {
                    countDownLatch.countDown();
                    super.jobInstanceScheduled(jobDefId, jobDefId, time);
                  }
                }));
    cut.jobInstanceScheduled(jobDefId, jobInsId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobInstanceScheduledException() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobInstanceScheduled(UUID jobDefId, UUID jobInsId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.jobInstanceScheduled(jobDefId, jobInsId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobInstanceStarted() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobInstanceStarted(
                      UUID jobDefId, UUID jobInsId, UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    super.jobInstanceStarted(jobDefId, jobDefId, nodeId, time);
                  }
                }));
    cut.jobInstanceStarted(jobDefId, jobInsId, nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobInstanceStartedException() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobInstanceStarted(
                      UUID jobDefId, UUID jobInsId, UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.jobInstanceStarted(jobDefId, jobInsId, nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobInstanceCompleted() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobInstanceCompleted(
                      UUID jobDefId, UUID jobInsId, UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    super.jobInstanceCompleted(jobDefId, jobDefId, nodeId, time);
                  }
                }));
    cut.jobInstanceCompleted(jobDefId, jobInsId, nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobInstanceCompletedException() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobInstanceCompleted(
                      UUID jobDefId, UUID jobInsId, UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.jobInstanceCompleted(jobDefId, jobInsId, nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobInstanceFailed() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobInstanceFailed(
                      UUID jobDefId, UUID jobInsId, UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    super.jobInstanceFailed(jobDefId, jobDefId, nodeId, time);
                  }
                }));
    cut.jobInstanceFailed(jobDefId, jobInsId, nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobInstanceFailedException() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    UUID nodeId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobInstanceFailed(
                      UUID jobDefId, UUID jobInsId, UUID nodeId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.jobInstanceFailed(jobDefId, jobInsId, nodeId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobInstanceRemoved() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobInstanceRemoved(UUID jobDefId, UUID jobInsId, Instant time) {
                    countDownLatch.countDown();
                    super.jobInstanceRemoved(jobDefId, jobDefId, time);
                  }
                }));
    cut.jobInstanceRemoved(jobDefId, jobInsId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }

  @Test
  void testJobInstanceRemovedException() throws InterruptedException {
    UUID jobDefId = UUID.randomUUID();
    UUID jobInsId = UUID.randomUUID();
    Instant now = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    CompositeCloudSchedulerObserver cut =
        new CompositeCloudSchedulerObserver(
            Arrays.asList(
                new AbstractCloudSchedulerObserver() {
                  @Override
                  public void jobInstanceRemoved(UUID jobDefId, UUID jobInsId, Instant time) {
                    countDownLatch.countDown();
                    throw new RuntimeException();
                  }
                }));
    cut.jobInstanceRemoved(jobDefId, jobInsId, now);
    countDownLatch.await(2, TimeUnit.SECONDS);
  }
}
