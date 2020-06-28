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

package io.github.cloudscheduler.spring;

import io.github.cloudscheduler.Job;
import io.github.cloudscheduler.JobFactory;
import io.github.cloudscheduler.JobScheduleCalculator;
import io.github.cloudscheduler.model.JobDefinition;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Cloud scheduler spring framework job factory implementation. Use spring application context to
 * create job instance.
 *
 * @author Wei Gao
 */
public class AutowiringSpringBeanJobFactory implements JobFactory, ApplicationContextAware {

  private ApplicationContext appContext;

  @Override
  public Job newJob(JobDefinition jobDefinition) {
    Class<? extends Job> clazz = jobDefinition.getJobClass();
    return appContext.getBean(clazz);
  }

  @Override
  public JobScheduleCalculator createJobScheduleCalculator(
      Class<? extends JobScheduleCalculator> calculatorClass) {
    return appContext.getBean(calculatorClass);
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.appContext = applicationContext;
  }
}
