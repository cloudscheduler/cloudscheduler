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

package io.github.cloudscheduler.codec;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cloudscheduler.Job;
import io.github.cloudscheduler.JobExecutionContext;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionState;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import io.github.cloudscheduler.model.JobRunStatus;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public abstract class AbstractCodecProviderTest {
  protected abstract EntityCodecProvider getCodecProvider();

  @Test
  public void testEncodeJobDefinition() throws IOException {
    JobDefinition job =
        JobDefinition.newBuilder(TestJob.class)
            .startAt(Instant.now().plusSeconds(5))
            .endAt(Instant.now().plus(Duration.ofDays(20)))
            .name("TestJob")
            .fixedRate(Duration.ofHours(2))
            .repeat(500)
            .global()
            .allowDupInstances()
            .build();

    EntityEncoder<JobDefinition> encoder = getCodecProvider().getEntityEncoder(JobDefinition.class);
    assertThat(encoder).isNotNull();
    byte[] data = encoder.encode(job);
    assertThat(data).isNotEmpty();
  }

  @Test
  public void testEncodeDecodeJobDefinition1() throws IOException {
    JobDefinition job =
        JobDefinition.newBuilder(TestJob.class)
            .startAt(Instant.now().plusSeconds(5))
            .endAt(Instant.now().plus(Duration.ofDays(20)))
            .name("TestJob")
            .fixedRate(Duration.ofHours(2))
            .repeat(500)
            .global()
            .allowDupInstances()
            .build();

    EntityCodec<JobDefinition> codec = getCodecProvider().getEntityCodec(JobDefinition.class);
    assertThat(codec).isNotNull();
    byte[] data = codec.encode(job);
    assertThat(data).isNotEmpty();
    JobDefinition job1 = codec.decode(data);
    assertThat(job1).isEqualToIgnoringGivenFields(job, "data");
  }

  @Test
  public void testEncodeDecodeJobDefinition2() throws IOException {
    JobDefinition job =
        JobDefinition.newBuilder(TestJob.class)
            .cron("5 10 * * *")
            .name("TestJob")
            .repeat(500)
            .jobData("Initial value", 10)
            .build();

    EntityCodec<JobDefinition> codec = getCodecProvider().getEntityCodec(JobDefinition.class);
    assertThat(codec).isNotNull();
    byte[] data = codec.encode(job);
    assertThat(data).isNotEmpty();
    JobDefinition job1 = codec.decode(data);
    assertThat(job1).isEqualToComparingFieldByField(job);
  }

  @Test
  public void testEncodeDecodeJobDefinition3() throws IOException {
    JobDefinition job =
        JobDefinition.newBuilder(TestJob.class)
            .initialDelay(Duration.ofSeconds(15))
            .fixedDelay(Duration.ofMinutes(50))
            .name("TestJob")
            .jobData("Initial value", true)
            .build();

    EntityCodec<JobDefinition> codec = getCodecProvider().getEntityCodec(JobDefinition.class);
    assertThat(codec).isNotNull();
    byte[] data = codec.encode(job);
    assertThat(data).isNotEmpty();
    JobDefinition job1 = codec.decode(data);
    assertThat(job1).isEqualToComparingFieldByField(job);
  }

  @Test
  public void testEncodeJobDefinitionStatus() throws IOException {
    JobDefinitionStatus status = new JobDefinitionStatus(UUID.randomUUID());
    status.setState(JobDefinitionState.CREATED);
    status.setLastScheduleTime(Instant.now());
    status.setRunCount(10);

    EntityEncoder<JobDefinitionStatus> encoder =
        getCodecProvider().getEntityEncoder(JobDefinitionStatus.class);
    assertThat(encoder).isNotNull();
    byte[] data = encoder.encode(status);
    assertThat(data).isNotEmpty();
  }

  @Test
  public void testEncodeDecodeJobDefinitionStatus() throws IOException {
    JobDefinitionStatus status = new JobDefinitionStatus(UUID.randomUUID());
    status.setState(JobDefinitionState.CREATED);
    status.setLastScheduleTime(Instant.now());
    status.setRunCount(10);
    status.setLastCompleteTime(Instant.now().plusSeconds(500));

    EntityCodec<JobDefinitionStatus> codec =
        getCodecProvider().getEntityCodec(JobDefinitionStatus.class);
    assertThat(codec).isNotNull();
    byte[] data = codec.encode(status);
    assertThat(data).isNotEmpty();
    JobDefinitionStatus status1 = codec.decode(data);
    assertThat(status1).isEqualToComparingFieldByField(status);
  }

  @Test
  public void testEncodeJobInstance() throws IOException {
    JobInstance instance = new JobInstance(UUID.randomUUID());
    instance.setScheduledTime(Instant.now());
    instance.setJobState(JobInstanceState.COMPLETE);

    EntityEncoder<JobInstance> encoder = getCodecProvider().getEntityEncoder(JobInstance.class);
    assertThat(encoder).isNotNull();
    byte[] data = encoder.encode(instance);
    assertThat(data).isNotEmpty();
  }

  @Test
  public void testEncodeDecodeJobInstance() throws IOException {
    JobInstance instance = new JobInstance(UUID.randomUUID());
    instance.setScheduledTime(Instant.now());
    instance.setJobState(JobInstanceState.COMPLETE);
    JobRunStatus runStatus = new JobRunStatus(UUID.randomUUID());
    runStatus.setStartTime(Instant.now());
    runStatus.setState(JobInstanceState.RUNNING);
    instance.getRunStatus().put(runStatus.getNodeId(), runStatus);

    EntityCodec<JobInstance> codec = getCodecProvider().getEntityCodec(JobInstance.class);
    assertThat(codec).isNotNull();
    byte[] data = codec.encode(instance);
    assertThat(data).isNotEmpty();
    JobInstance instance1 = codec.decode(data);
    assertThat(instance1).usingRecursiveComparison().isEqualTo(instance);
  }

  @Test
  public void testEncodeDecodeNode() throws IOException {
    Node node = new Node(UUID.randomUUID());

    EntityCodec<Node> codec = getCodecProvider().getEntityCodec(Node.class);
    assertThat(codec).isNotNull();
    byte[] data = codec.encode(node);
    assertThat(data).isNotEmpty();
    Node node1 = codec.decode(data);
    assertThat(node1).isEqualToComparingFieldByField(node);
  }

  public static final class TestJob implements Job {
    @Override
    public void execute(JobExecutionContext ctx) {}
  }
}
