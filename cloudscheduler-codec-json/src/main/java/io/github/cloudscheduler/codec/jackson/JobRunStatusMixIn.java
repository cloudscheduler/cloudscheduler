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

package io.github.cloudscheduler.codec.jackson;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.github.cloudscheduler.model.JobInstanceState;

import java.time.Instant;
import java.util.UUID;

/**
 * JobRunStatus mixin definition for jackson.
 *
 * @author Wei Gao
 */
abstract class JobRunStatusMixIn {
  JobRunStatusMixIn(@JsonProperty("node_id") UUID nodeId) {
  }

  @JsonProperty("node_id")
  abstract UUID getNodeId();

  @JsonProperty("start_time")
  abstract Instant getStartTime();

  @JsonProperty("start_time")
  abstract void setStartTime(Instant startTime);

  @JsonProperty("finish_time")
  abstract Instant getFinishTime();

  @JsonProperty("finish_time")
  abstract void setFinishTime(Instant finishTime);

  @JsonProperty("state")
  abstract JobInstanceState getState();

  @JsonProperty("state")
  abstract void setState(JobInstanceState state);
}
