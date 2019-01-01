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

import io.github.cloudscheduler.codec.AbstractCodecProviderTest;
import io.github.cloudscheduler.codec.EntityCodecProvider;
import io.github.cloudscheduler.codec.EntityDecoder;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;

import java.io.IOException;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

public class JacksonCodecProviderTest extends AbstractCodecProviderTest {
  private final static EntityCodecProvider codecProvider = new JacksonCodecProvider();

  @Override
  protected EntityCodecProvider getCodecProvider() {
    return codecProvider;
  }

  @Test
  public void testDecodeJobDefinition() throws IOException {
    String jsonStr = "{\"id\": \"cd8c9c85-760c-4511-a5da-4ef537d0da77\",\"job_class\":\"io.github.cloudscheduler.codec.AbstractCodecProviderTest$TestJob\"}";
    EntityDecoder<JobDefinition> decoder;
    decoder = getCodecProvider().getEntityDecoder(JobDefinition.class);
    Assert.assertNotNull(decoder);
    JobDefinition job = decoder.decode(jsonStr.getBytes("UTF-8"));
    Assert.assertNotNull(job);
    Assert.assertEquals(job.getId(), UUID.fromString("cd8c9c85-760c-4511-a5da-4ef537d0da77"));
    Assert.assertEquals(job.getJobClass(), TestJob.class);
  }

  @Test
  public void testDecodeJobDefinitionStatus() throws IOException {
    String jsonStr = "{\"jobDefId\": \"cd8c9c85-760c-4511-a5da-4ef537d0da77\"}";
    EntityDecoder<JobDefinitionStatus> decoder = getCodecProvider().getEntityDecoder(JobDefinitionStatus.class);
    Assert.assertNotNull(decoder);
    JobDefinitionStatus status = decoder.decode(jsonStr.getBytes("UTF-8"));
    Assert.assertNotNull(status);
    Assert.assertEquals(status.getId(), UUID.fromString("cd8c9c85-760c-4511-a5da-4ef537d0da77"));
  }

  @Test
  public void testDecodeJobInstance() throws IOException {
    String jsonStr = "{\"id\": \"cd8c9c85-760c-4511-a5da-4ef537d0da77\",\"definition_id\":\"71275b60-c38c-4afc-a4f2-ed49e30de19d\"}";
    EntityDecoder<JobInstance> decoder = getCodecProvider().getEntityDecoder(JobInstance.class);
    Assert.assertNotNull(decoder);
    JobInstance instance = decoder.decode(jsonStr.getBytes("UTF-8"));
    Assert.assertNotNull(instance);
    Assert.assertEquals(instance.getId(), UUID.fromString("cd8c9c85-760c-4511-a5da-4ef537d0da77"));
    Assert.assertEquals(instance.getJobDefId(), UUID.fromString("71275b60-c38c-4afc-a4f2-ed49e30de19d"));
  }
}
