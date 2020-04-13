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

import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This is main entry for cloud scheduler.
 *
 * @author Wei Gao
 */
public interface Scheduler {
  /**
   * Run a job at given time.
   *
   * @param jobClass Job class
   * @param time run time
   * @return job definition
   */
  JobDefinition runOnce(Class<? extends Job> jobClass, Instant time);

  /**
   * Run a job now.
   *
   * @param jobClass Job class
   * @return job definition
   */
  JobDefinition runNow(Class<? extends Job> jobClass);

  /**
   * Schedule a job. Use {@link JobDefinition.Builder} to build job definition.
   *
   * @param job JobDefinition
   * @see JobDefinition.Builder
   */
  void schedule(JobDefinition job);

  /**
   * Pause a job.
   *
   * @param id JobDefinition id
   * @param mayInterrupt interrupt job if it's running
   * @return job definition
   */
  JobDefinition pause(UUID id, boolean mayInterrupt);

  /**
   * Resume a paused job.
   *
   * @param id JobDefinition id
   * @return job definition
   */
  JobDefinition resume(UUID id);

  /**
   * List all JobDefinition by name.
   *
   * @param name JobDefinition name
   * @return list of JobDefinition
   */
  List<JobDefinition> listJobDefinitionsByName(String name);

  /**
   * List all JobDefinitions.
   *
   * @return list of JobDefinition
   */
  List<JobDefinition> listJobDefinitions();

  /**
   * List all job definitions with status.
   *
   * @return list of job definition.
   */
  Map<JobDefinition, JobDefinitionStatus> listJobDefinitionsWithStatus();

  /**
   * Delete a job definition.
   *
   * @param id job definition id
   * @return deleted job definition
   */
  JobDefinition delete(UUID id);
}
