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
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.UUID;

/**
 * <p>Cloud scheduler observer. Cloud scheduler will call methods on observer based on events.</p>
 *
 * <p>Cloud scheduler use {@link ServiceLoader} to find all implementations.
 * Please refer to {@link ServiceLoader} javadoc on how to register your implementation
 * of CloudSchedulerObserver.</p>
 *
 * <p>Cloud scheduler event will be dispatched by single thread, slow implementation could
 * block events got dispatched.</p>
 *
 * @author Wei Gao
 */
public interface CloudSchedulerObserver {
  /**
   * Use service loader to initial cloud scheduler observers.
   *
   * @return cloud scheduler observer
   */
  static List<CloudSchedulerObserver> getCloudSchedulerObserver() {
    ServiceLoader<CloudSchedulerObserver> serviceLoader =
        ServiceLoader.load(CloudSchedulerObserver.class);
    List<CloudSchedulerObserver> observers = new ArrayList<>();
    serviceLoader.forEach(observers::add);
    return observers;
  }

  /**
   * Been called while a master node up. (On master node)
   *
   * @param nodeId master node id
   * @param time   master node start time
   */
  void masterNodeUp(UUID nodeId, Instant time);

  /**
   * Been called while a master node down. (On master node)
   *
   * @param nodeId master node id
   * @param time   master node down time
   */
  void masterNodeDown(UUID nodeId, Instant time);

  /**
   * Been called while a worker node up. (On a worker node)
   *
   * @param nodeId worker node id
   * @param time   worker node up time
   */
  void workerNodeUp(UUID nodeId, Instant time);

  /**
   * Been called while a worker node down. (On a worker node)
   *
   * @param nodeId worker node id
   * @param time   worker node down time
   */
  void workerNodeDown(UUID nodeId, Instant time);

  /**
   * Been called after a worker node been removed. (On master node)
   *
   * @param nodeId worker node id
   * @param time   worker node down time
   */
  void workerNodeRemoved(UUID nodeId, Instant time);

  /**
   * A job definition been paused. (On master node)
   *
   * @param id   job definition id
   * @param time job definition been paused time
   */
  void jobDefinitionPaused(UUID id, Instant time);

  /**
   * A job definition resumed from pause. (On master node)
   *
   * @param id   job definition id
   * @param time job definition resumed time
   */
  void jobDefinitionResumed(UUID id, Instant time);

  /**
   * A job definition completed. This method will be called before last job instance complete.
   * (On master node)
   *
   * @param id   job definition id
   * @param time job definition finished time
   */
  void jobDefinitionCompleted(UUID id, Instant time);

  /**
   * A job definition been removed. (On master node)
   *
   * @param id   job definition id
   * @param time job definition been removed time
   */
  void jobDefinitionRemoved(UUID id, Instant time);

  /**
   * A job instance been scheduled. (On master node)
   *
   * @param jobDefId job definition id
   * @param jobInId  job instance id
   * @param time     job instance been scheduled time
   */
  void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time);

  /**
   * A job instance has started. (On worker node)
   *
   * @param jobDefId job definition id
   * @param jobInId  job instance id
   * @param nodeId   job instance starting on node id
   * @param time     job instance starting time
   */
  void jobInstanceStarted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time);

  /**
   * A job instance completed. (On worker node)
   *
   * @param jobDefId job definition id
   * @param jobInId  job instance id
   * @param nodeId   job instance complete on node id
   * @param time     job instance complete time
   */
  void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time);

  /**
   * A job instance failed. (On worker node)
   *
   * @param jobDefId job definition id
   * @param jobInId  job instance id
   * @param nodeId   job instance failed on node id
   * @param time     job instance failed time
   */
  void jobInstanceFailed(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time);

  /**
   * A job instance been removed after cleanup. (On master node)
   *
   * @param jobDefId job definition id
   * @param jobInId  job instance id
   * @param time     job instance removed time
   */
  void jobInstanceRemoved(UUID jobDefId, UUID jobInId, Instant time);
}
