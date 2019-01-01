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

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A compose {@link CloudSchedulerObserver} implementation. This compose observer will
 * use a separate thread to dispatch event to a chain of observers.
 *
 * @author Wei Gao
 */
class CompositeCloudSchedulerObserver implements CloudSchedulerObserver {
  private static final Logger logger = LoggerFactory
      .getLogger(CompositeCloudSchedulerObserver.class);

  private final List<CloudSchedulerObserver> observers;
  private final ExecutorService threadPool;

  CompositeCloudSchedulerObserver(List<CloudSchedulerObserver> observers) {
    this.observers = observers;
    threadPool = Executors.newFixedThreadPool(1, r ->
        new Thread(r, "CloudSchedulerObserver-deliver"));
  }

  @Override
  public void masterNodeUp(UUID nodeId, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.masterNodeUp(nodeId, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify master node up, ignore it.", e);
      }
    }));
  }

  @Override
  public void masterNodeDown(UUID nodeId, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.masterNodeDown(nodeId, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify master node down, ignore it.", e);
      }
    }));
  }

  @Override
  public void workerNodeUp(UUID nodeId, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.workerNodeUp(nodeId, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify worker node up, ignore it.", e);
      }
    }));
  }

  @Override
  public void workerNodeDown(UUID nodeId, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.workerNodeDown(nodeId, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify worker node down, ignore it.", e);
      }
    }));
  }

  @Override
  public void workerNodeRemoved(UUID nodeId, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.workerNodeRemoved(nodeId, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify worker node been removed, ignore it.", e);
      }
    }));
  }

  @Override
  public void jobDefinitionPaused(UUID id, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.jobDefinitionPaused(id, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify job definition been paused, ignore it.", e);
      }
    }));
  }

  @Override
  public void jobDefinitionResumed(UUID id, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.jobDefinitionResumed(id, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify job definition been resumed, ignore it.", e);
      }
    }));
  }

  @Override
  public void jobDefinitionCompleted(UUID id, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.jobDefinitionCompleted(id, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify job definition been completed, ignore it.", e);
      }
    }));
  }

  @Override
  public void jobDefinitionRemoved(UUID id, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.jobDefinitionRemoved(id, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify job definition been removed, ignore it.", e);
      }
    }));
  }

  @Override
  public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.jobInstanceScheduled(jobDefId, jobInId, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify job instance been scheduled, ignore it.", e);
      }
    }));
  }

  @Override
  public void jobInstanceStarted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.jobInstanceStarted(jobDefId, jobInId, nodeId, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify job instance started, ignore it.", e);
      }
    }));
  }

  @Override
  public void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.jobInstanceCompleted(jobDefId, jobInId, nodeId, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify job instance completed, ignore it.", e);
      }
    }));
  }

  @Override
  public void jobInstanceFailed(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.jobInstanceFailed(jobDefId, jobInId, nodeId, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify job instance failed, ignore it.", e);
      }
    }));
  }

  @Override
  public void jobInstanceRemoved(UUID jobDefId, UUID jobInId, Instant time) {
    threadPool.submit(() -> observers.forEach(observer -> {
      try {
        observer.jobInstanceRemoved(jobDefId, jobInId, time);
      } catch (Throwable e) {
        logger.debug("Error happened when notify job instance been removed, ignore it.", e);
      }
    }));
  }
}
