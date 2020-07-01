package io.github.cloudscheduler.spring;

import io.github.cloudscheduler.Job;
import io.github.cloudscheduler.JobScheduleCalculator;
import io.github.cloudscheduler.model.JobDefinition;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Tested;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

public class SpringBeanJobFactoryTest {
  @Tested(availableDuringSetup = true)
  private AutowiringSpringBeanJobFactory cut;

  @Mocked private ApplicationContext appContext;

  @BeforeEach
  public void init() {
    cut.setApplicationContext(appContext);
  }

  @Test
  public void testNewJob(@Mocked JobDefinition jobDefinition, @Mocked Job job) {
    new Expectations() {
      {
        jobDefinition.getJobClass();
        result = Job.class;
        appContext.getBean(Job.class);
        result = job;
      }
    };
    assertThat(cut.newJob(jobDefinition)).isNotNull().isSameAs(job);
  }

  @Test
  public void testCreateJobScheduleCalculator(@Mocked JobScheduleCalculator calculator) {
    new Expectations() {
      {
        appContext.getBean(JobScheduleCalculator.class);
        result = calculator;
      }
    };
    assertThat(cut.createJobScheduleCalculator(JobScheduleCalculator.class))
        .isNotNull()
        .isSameAs(calculator);
  }
}
