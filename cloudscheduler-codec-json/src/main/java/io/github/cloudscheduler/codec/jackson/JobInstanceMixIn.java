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
import io.github.cloudscheduler.model.JobRunStatus;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * JobInstance mixin definition for jackson.
 *
 * @author Wei Gao
 */
abstract class JobInstanceMixIn {
  JobInstanceMixIn(@JsonProperty("id") UUID id,
                   @JsonProperty("definition_id") UUID jobDefId) {
  }

  @JsonProperty("id")
  abstract UUID getId();

  @JsonProperty("definition_id")
  abstract UUID getJobDefId();

  abstract Map<UUID, JobRunStatus> getRunStatus();

  @JsonProperty("scheduled_time")
  abstract Instant getScheduledTime();

  @JsonProperty("scheduled_time")
  abstract void setScheduledTime(Instant scheduledTime);

  @JsonProperty("job_state")
  abstract JobInstanceState getJobState();

  @JsonProperty("job_state")
  abstract void setJobState(JobInstanceState jobState);
}
