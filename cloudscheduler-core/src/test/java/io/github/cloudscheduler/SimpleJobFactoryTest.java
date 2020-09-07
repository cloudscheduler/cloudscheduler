/*
 *
 * Copyright (c) 2020. cloudscheduler
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
 *
 */

package io.github.cloudscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Tested;
import org.junit.jupiter.api.Test;

public class SimpleJobFactoryTest {
  @Tested private SimpleJobFactory cut;

  @Test
  public void testNewJob(@Mocked JobDefinition jobDef) throws Exception {
    new Expectations() {
      {
        jobDef.getJobClass();
        result = TestJob.class;
      }
    };
    assertThat(cut.newJob(jobDef)).isNotNull().isInstanceOf(TestJob.class);
  }

  @Test
  public void testCreateJobScheduleCalculator() throws Throwable {
    assertThat(cut.createJobScheduleCalculator(TestCalculator.class))
        .isNotNull()
        .isInstanceOf(JobScheduleCalculator.class);
  }

  @Test
  public void testCreateJobScheduleCalculatorException(@Mocked TestCalculator calculator) {
    new Expectations() {
      {
        new TestCalculator();
        result = new NoSuchMethodException();
      }
    };
    assertThatExceptionOfType(InvocationTargetException.class)
        .isThrownBy(() -> cut.createJobScheduleCalculator(TestCalculator.class));
  }

  @Test
  public void testCreateJobScheduleCalculatorReuse(@Mocked TestCalculator calculator)
      throws Throwable {
    AtomicInteger instanceCount = new AtomicInteger(0);
    new MockUp<TestCalculator>() {
      @Mock
      public void $init() {
        instanceCount.incrementAndGet();
      }
    };
    JobScheduleCalculator c = cut.createJobScheduleCalculator(TestCalculator.class);
    assertThat(c).isNotNull();
    assertThat(instanceCount).hasValue(1);
    JobScheduleCalculator c1 = cut.createJobScheduleCalculator(TestCalculator.class);
    assertThat(c1).isSameAs(c);
    assertThat(instanceCount).hasValue(1);
  }

  private static class TestCalculator implements JobScheduleCalculator {
    public TestCalculator() {}

    @Override
    public Instant calculateNextRunTime(JobDefinition jobDefinition, JobDefinitionStatus status) {
      return null;
    }
  }
}
