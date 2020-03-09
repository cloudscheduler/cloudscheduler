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

package io.github.cloudscheduler.model;

import io.github.cloudscheduler.Job;
import io.github.cloudscheduler.util.CronExpression;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * <p>JobDefinition. End user should use {@link Builder} to create instance of JobDefinition.</p>
 *
 * <p>User create new scheduled job by create instance of JobDefinition.</p>
 *
 * @author Wei Gao
 */
public class JobDefinition implements Serializable {
  private static final Duration MINIMUM_RATE = Duration.ofSeconds(5);
  // Unique id of this job definition
  private final UUID id;
  // Job class, must implement Job interface
  private final Class<? extends Job> jobClass;
  // Job context data, will be pass in Job Instance
  private final Map<String, Object> data;
  // Job definition name. may not unique.
  private final String name;
  // Job mode
  private final ScheduleMode mode;
  // Cron if mode is CRON
  private final String cron;
  // Start time if mode is START_AT
  private final Instant startTime;
  // Job fixed rate, mode is START_AT or START_NOW
  private final Duration rate;
  // Job fixed delay, mode is START_AT or START_NOW, exclusive with rate
  private final Duration delay;
  // Job repeat times
  private final Integer repeat;
  // Job stop time.
  private final Instant endTime;
  // If this is a global job.
  private final boolean global;
  // If allow duplicate instance (multiple job run concurrently)
  private final boolean allowDupInstances;

  JobDefinition(UUID id,
                Class<? extends Job> jobClass,
                String name,
                Map<String, Object> data,
                ScheduleMode mode,
                String cron,
                Instant startTime,
                Instant endTime,
                Duration rate,
                Duration delay,
                Integer repeat,
                boolean global,
                boolean allowDupInstances) {
    this.id = id;
    this.name = name;
    this.jobClass = jobClass;
    this.data = data;
    this.mode = mode;
    this.startTime = startTime;
    this.endTime = endTime;
    this.cron = cron;
    this.rate = rate;
    this.delay = delay;
    this.repeat = repeat;
    this.global = global;
    this.allowDupInstances = allowDupInstances;
  }

  public UUID getId() {
    return id;
  }

  public Class<? extends Job> getJobClass() {
    return jobClass;
  }

  public String getName() {
    return name;
  }

  public Map<String, Object> getData() {
    return data == null ? null : Collections.unmodifiableMap(data);
  }

  public ScheduleMode getMode() {
    return mode;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public Instant getEndTime() {
    return endTime;
  }

  public String getCron() {
    return cron;
  }

  public Duration getRate() {
    return rate;
  }

  public Duration getDelay() {
    return delay;
  }

  public Integer getRepeat() {
    return repeat;
  }

  public boolean isGlobal() {
    return global;
  }

  public boolean isAllowDupInstances() {
    return allowDupInstances;
  }

  @Override
  public int hashCode() {
    return 7 + (id.hashCode() * 13);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof JobDefinition)) {
      return false;
    }
    final JobDefinition obj = (JobDefinition) other;
    return id.equals(obj.getId());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("JobDefinition[");
    sb.append("id=\"").append(id.toString()).append("\"")
        .append(",name=\"").append(name).append("\"")
        .append(",mode=\"").append(mode).append("\"");
    switch (mode) {
      case CRON:
        sb.append(",cron=\"").append(cron).append("\"");
        break;
      case START_AT:
        sb.append(",start=\"").append(startTime).append("\"");
        if (rate != null) {
          sb.append(",rate=\"").append(rate).append("\"");
        }
        if (delay != null) {
          sb.append(",delay=\"").append(delay).append("\"");
        }
        break;
      default:
        if (rate != null) {
          sb.append(",rate=\"").append(rate).append("\"");
        }
        if (delay != null) {
          sb.append(",delay=\"").append(delay).append("\"");
        }
    }
    if (repeat != null) {
      sb.append(",repeat=").append(repeat);
    }
    if (endTime != null) {
      sb.append(",end=\"").append(endTime).append("\"");
    }
    if (global) {
      sb.append(",global");
    }
    if (allowDupInstances) {
      sb.append(",allowDuplicateInstances");
    }
    sb.append("]");
    return sb.toString();
  }

  public enum ScheduleMode {
    START_NOW, START_AT, CRON
  }

  /**
   * Create new JobDefinition Builder instance with gaven Job Class.
   *
   * @param jobClass User's implementation of Job
   * @return JobDefinition Builder object
   */
  public static Builder newBuilder(Class<? extends Job> jobClass) {
    Objects.requireNonNull(jobClass, "Job class is mandatory");
    return new Builder(jobClass);
  }

  /**
   * Builder class for JobDefinition.
   */
  public static final class Builder {
    private final Class<? extends Job> jobClass;
    private final Map<String, Object> data;
    private String name;
    private ScheduleMode mode;
    private String cron;
    private Instant startTime;
    private Instant endTime;
    private Duration rate;
    private Duration delay;
    private Integer repeat;
    private boolean global;
    private boolean allowDupInstances;

    private Builder(Class<? extends Job> jobClass) {
      this.jobClass = jobClass;
      data = new HashMap<>();
      mode = ScheduleMode.START_NOW;
      global = false;
    }

    /**
     * Give a name to this JobDefinition. JobDefinition name don't require to be unique.
     *
     * @param name JobDefinition name
     * @return JobDefinition Builder object
     */
    public Builder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * Adding job data. Job data will be passed as part of job execution context when job get
     * executed
     *
     * @param key   Key
     * @param value Value
     * @return JobDefinition Builder object
     */
    public Builder jobData(String key, Object value) {
      data.put(key, value);
      return this;
    }

    /**
     * Schedule job start immediately.
     *
     * @return JobDefinition Builder object
     */
    public Builder runNow() {
      mode = ScheduleMode.START_NOW;
      cron = null;
      startTime = null;
      return this;
    }

    /**
     * Schedule job start at gaven time.
     *
     * @param startTime Start time
     * @return JobDefinition Builder object
     */
    public Builder startAt(Instant startTime) {
      if (startTime == null) {
        throw new NullPointerException("Start time is mandatory");
      }
      mode = ScheduleMode.START_AT;
      cron = null;
      this.startTime = startTime;
      return this;
    }

    /**
     * Schedule job start at gaven initial delay.
     *
     * @param initialDelay Initial delay
     * @return JobDefinition Builder object
     */
    public Builder initialDelay(Duration initialDelay) {
      if (initialDelay == null) {
        throw new NullPointerException("Initial delay is mandatory");
      }
      if (initialDelay.isNegative() || initialDelay.isZero()
          || initialDelay.compareTo(MINIMUM_RATE) < 0) {
        throw new IllegalArgumentException("Initial delay cannot be negative, zero or less than "
            + MINIMUM_RATE);
      }
      mode = ScheduleMode.START_AT;
      Instant startTime = Instant.now().plus(initialDelay);
      cron = null;
      this.startTime = startTime;
      return this;
    }

    /**
     * Schedule job with fixed rate.
     *
     * @param rate Rate
     * @return JobDefinition Builder object
     */
    public Builder fixedRate(Duration rate) {
      if (ScheduleMode.CRON.equals(mode)) {
        throw new IllegalArgumentException("Rate cannot apply to cron job");
      }
      if (rate == null) {
        throw new NullPointerException("Rate is mandatory");
      }
      if (rate.isNegative() || rate.isZero() || rate.compareTo(MINIMUM_RATE) < 0) {
        throw new IllegalArgumentException("Rate cannot be negative, zero or less than 1 minute");
      }
      this.rate = rate;
      this.delay = null;
      return this;
    }

    /**
     * Schedule job with fixed delay.
     *
     * @param delay Delay
     * @return JobDefinition Builder object
     */
    public Builder fixedDelay(Duration delay) {
      if (ScheduleMode.CRON.equals(mode)) {
        throw new IllegalArgumentException("Delay cannot apply to cron job");
      }
      if (delay == null) {
        throw new NullPointerException("Delay is mandatory");
      }
      if (delay.isNegative() || delay.isZero() || delay.compareTo(MINIMUM_RATE) < 0) {
        throw new IllegalArgumentException("Delay cannot be negative, zero or less than 1 minute");
      }
      this.delay = delay;
      this.rate = null;
      return this;
    }

    /**
     * Schedule job that repeat for certain times.
     *
     * @param repeat Repeat times
     * @return JobDefinition Builder object
     */
    public Builder repeat(int repeat) {
      if (repeat <= 0) {
        throw new IllegalArgumentException("Repeat must greater than 0");
      }
      if (!ScheduleMode.CRON.equals(mode) && rate == null && delay == null) {
        throw new IllegalStateException("Cannot repeat without rate or delay");
      }
      this.repeat = repeat;
      return this;
    }

    /**
     * Schedule job that end at specific time.
     *
     * @param endTime End time
     * @return JobDefinition Builder object
     */
    public Builder endAt(Instant endTime) {
      this.endTime = endTime;
      return this;
    }

    /**
     * Schedule job as global job. Every current worker node will execute the job
     *
     * @return JobDefinition Builder object
     */
    public Builder global() {
      return global(true);
    }

    /**
     * Schedule job as global or no global job.
     *
     * @param global If this is a global job
     * @return JobDefinition Builder object
     */
    public Builder global(boolean global) {
      this.global = global;
      return this;
    }

    /**
     * Schedule job with cron expression.
     *
     * @param cron cron expression
     * @return JobDefinition Builder object
     */
    public Builder cron(String cron) {
      if (cron == null) {
        throw new NullPointerException("Cron is mandatory");
      }
      // Validate cron string
      CronExpression.createWithoutSeconds(cron);
      mode = ScheduleMode.CRON;
      this.cron = cron;
      startTime = null;
      rate = null;
      delay = null;
      return this;
    }

    /**
     * Allow duplicate job instance. Start next one even previous not complete yet.
     *
     * @return JobDefinition Builder object
     */
    public Builder allowDupInstances() {
      return allowDupInstances(true);
    }

    /**
     * Allow duplicate job instance or not.
     *
     * @param allowDupInstances Allow duplicate or not
     * @return JobDefinition Builder object
     */
    public Builder allowDupInstances(boolean allowDupInstances) {
      if (delay != null) {
        throw new IllegalArgumentException(
            "Allow duplicate instance cannot apply to fix delay job.");
      }
      this.allowDupInstances = allowDupInstances;
      return this;
    }

    /**
     * Build JobDefinition.
     *
     * @return JobDefinition instance
     */
    public JobDefinition build() {
      UUID id = UUID.randomUUID();
      if (name == null) {
        name = id.toString();
      }
      return new JobDefinition(id, jobClass, name, data, mode, cron,
          startTime, endTime, rate, delay, repeat, global, allowDupInstances);
    }
  }
}
