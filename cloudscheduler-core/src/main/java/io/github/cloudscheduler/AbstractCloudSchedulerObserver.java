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
import java.util.UUID;

/** An abstract implementation of CloudSchedulerObserver. */
public abstract class AbstractCloudSchedulerObserver implements CloudSchedulerObserver {
  @Override
  public void masterNodeUp(UUID nodeId, Instant time) {}

  @Override
  public void masterNodeDown(UUID nodeId, Instant time) {}

  @Override
  public void workerNodeUp(UUID nodeId, Instant time) {}

  @Override
  public void workerNodeDown(UUID nodeId, Instant time) {}

  @Override
  public void workerNodeRemoved(UUID nodeId, Instant time) {}

  @Override
  public void jobDefinitionPaused(UUID id, Instant time) {}

  @Override
  public void jobDefinitionResumed(UUID id, Instant time) {}

  @Override
  public void jobDefinitionRemoved(UUID id, Instant time) {}

  @Override
  public void jobInstanceScheduled(UUID jobDefId, UUID jobInId, Instant time) {}

  @Override
  public void jobInstanceStarted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {}

  @Override
  public void jobInstanceCompleted(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {}

  @Override
  public void jobInstanceFailed(UUID jobDefId, UUID jobInId, UUID nodeId, Instant time) {}

  @Override
  public void jobInstanceRemoved(UUID jobDefId, UUID jobInId, Instant time) {}

  @Override
  public void jobDefinitionCompleted(UUID id, Instant time) {}
}
