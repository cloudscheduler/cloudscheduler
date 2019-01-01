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

import io.github.cloudscheduler.Job;
import io.github.cloudscheduler.model.JobDefinition;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * JobDefinition mixin definition for jackson.
 *
 * @author Wei Gao
 */
abstract class JobDefinitionMixIn {
  JobDefinitionMixIn(@JsonProperty("id") UUID id,
                     @JsonProperty("job_class") Class<? extends Job> jobClass,
                     @JsonProperty("name") String name,
                     @JsonProperty("ctx_data") Map<String, Object> data,
                     @JsonProperty("mode") JobDefinition.ScheduleMode mode,
                     @JsonProperty("cron") String cron,
                     @JsonProperty("start_time") Instant startTime,
                     @JsonProperty("end_time") Instant endTime,
                     @JsonProperty("rate") Duration rate,
                     @JsonProperty("delay") Duration delay,
                     @JsonProperty("repeat") Integer repeat,
                     @JsonProperty("global") boolean global,
                     @JsonProperty("allow_dup_instances") boolean allowDupInstances) {
  }

  @JsonProperty("id")
  abstract UUID getId();

  @JsonProperty("job_class")
  abstract Class<? extends Job> getJobClass();

  @JsonProperty("name")
  abstract String getName();

  @JsonProperty("ctx_data")
  abstract Map<String, Object> getData();

  @JsonProperty("mode")
  abstract JobDefinition.ScheduleMode getMode();

  @JsonProperty("start_time")
  abstract Instant getStartTime();

  @JsonProperty("end_time")
  abstract Instant getEndTime();

  @JsonProperty("cron")
  abstract String getCron();

  @JsonProperty("rate")
  abstract Duration getRate();

  @JsonProperty("delay")
  abstract Duration getDelay();

  @JsonProperty("repeat")
  abstract Integer getRepeat();

  @JsonProperty("global")
  abstract boolean isGlobal();

  @JsonProperty("allow_dup_instances")
  abstract boolean isAllowDupInstances();
}
