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

package io.github.cloudscheduler.service;

import io.github.cloudscheduler.AbstractTest;
import io.github.cloudscheduler.TestJob;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionState;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import io.github.cloudscheduler.model.JobRunStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Wei Gao
 */
public class JobServiceTest extends AbstractTest {
  private static final Logger logger = LoggerFactory.getLogger(JobServiceTest.class);

  @Test
  public void testListAllJobDefinitions() throws Throwable {
    List<UUID> jobIds = new ArrayList<>(3);
    JobDefinition job = JobDefinition.newBuilder(TestJob.class).build();
    jobService.saveJobDefinition(job);
    jobIds.add(job.getId());
    job = JobDefinition.newBuilder(TestJob.class).build();
    jobService.saveJobDefinition(job);
    jobIds.add(job.getId());
    job = JobDefinition.newBuilder(TestJob.class).build();
    jobService.saveJobDefinition(job);
    jobIds.add(job.getId());

    List<JobDefinition> jobs = jobService.listAllJobDefinitions();
    Assert.assertNotNull(jobs);
    Assert.assertEquals(jobs.size(), 3);
    jobs.forEach(j -> Assert.assertTrue(jobIds.contains(j.getId())));
  }

  @Test
  public void testListJobDefByName() throws Throwable {
    List<JobDefinition> jobs = new ArrayList<>(2);
    JobDefinition job = JobDefinition.newBuilder(TestJob.class).name("testJob").build();
    jobService.saveJobDefinition(job);
    jobs.add(job);
    job = JobDefinition.newBuilder(TestJob.class).build();
    jobService.saveJobDefinition(job);
    job = JobDefinition.newBuilder(TestJob.class).build();
    jobService.saveJobDefinition(job);
    job = JobDefinition.newBuilder(TestJob.class).name("testJob").build();
    jobService.saveJobDefinition(job);
    jobs.add(job);

    List<JobDefinition> jobs1 = jobService.listJobDefinitionsByName("testJob");
    Assert.assertNotNull(jobs1);
    Assert.assertEquals(jobs1.size(), 2);
    Assert.assertTrue(jobs.containsAll(jobs1));
    Assert.assertTrue(jobs1.containsAll(jobs));
  }

  @Test
  public void testGetJobInstancesByJobDefId() throws Throwable {
    JobDefinition jobDef = JobDefinition.newBuilder(TestJob.class).build();
    jobService.saveJobDefinition(jobDef);
    List<UUID> jobInsIds = new ArrayList<>();
    JobInstance jobIns = jobService.scheduleJobInstance(jobDef);
    jobInsIds.add(jobIns.getId());
    JobDefinition jobDef2 = JobDefinition.newBuilder(TestJob.class).build();
    jobService.saveJobDefinition(jobDef2);
    jobService.scheduleJobInstance(jobDef2);
    jobIns = jobService.scheduleJobInstance(jobDef);
    jobInsIds.add(jobIns.getId());

    List<JobInstance> jobInstances = jobService.getJobInstancesByJobDef(jobDef);
    Assert.assertNotNull(jobInstances);
    Assert.assertEquals(jobInstances.size(), 2);
    jobInstances.forEach(ji -> Assert.assertTrue(jobInsIds.contains(ji.getId())));
  }

  @Test
  public void testStartJobInstance() throws Throwable {
    JobDefinition jobDef = JobDefinition.newBuilder(TestJob.class).build();
    jobService.saveJobDefinition(jobDef);
    logger.info("JobDefinition with id: {} saved", jobDef.getId());

    UUID nodeId = UUID.randomUUID();

    JobInstance jobIns = jobService.scheduleJobInstance(jobDef);
    logger.info("JobInstance with id: {} scheduled", jobIns.getId());
    JobDefinitionStatus jobDefinitionStatus = jobService.getJobStatusById(jobDef.getId());
    Assert.assertNotNull(jobDefinitionStatus);
    JobInstanceState state = jobDefinitionStatus.getJobInstanceState().get(jobIns.getId());
    Assert.assertNotNull(state);
    Assert.assertEquals(state, JobInstanceState.SCHEDULED);

    jobService.startProcessJobInstance(jobIns.getId(), nodeId);
    logger.info("JobInstance started with node id: {}", nodeId);

    JobInstance ji = jobService.getJobInstanceById(jobIns.getId());

    Assert.assertNotNull(ji);
    Assert.assertTrue(ji.getRunStatus().keySet().contains(nodeId));

    JobRunStatus status = ji.getRunStatus().get(nodeId);
    Assert.assertNotNull(status);
    Assert.assertEquals(status.getState(), JobInstanceState.RUNNING);

    jobService.completeJobInstance(jobIns.getId(), nodeId, JobInstanceState.FAILED);
    logger.info("JobInstance for nodeId: {} completed", nodeId);
    ji = jobService.getJobInstanceById(jobIns.getId());

    Assert.assertNotNull(ji);

    status = ji.getRunStatus().get(nodeId);
    Assert.assertNotNull(status);
    Assert.assertEquals(status.getState(), JobInstanceState.FAILED);
  }

  @Test
  public void testPauseJobDefinition() throws Throwable {
    JobDefinition jobDef = JobDefinition.newBuilder(TestJob.class).build();
    jobService.saveJobDefinition(jobDef);

    UUID nodeId = UUID.randomUUID();

    JobInstance jobIns = jobService.scheduleJobInstance(jobDef);

    jobService.pauseJob(jobDef.getId(), false);
    JobDefinition jd = jobService.getJobDefinitionById(jobDef.getId());
    Assert.assertNotNull(jd);
    JobDefinitionStatus status = jobService.getJobStatusById(jobDef.getId());
    Assert.assertNotNull(status);
    Assert.assertEquals(status.getState(), JobDefinitionState.PAUSED);
  }
}
