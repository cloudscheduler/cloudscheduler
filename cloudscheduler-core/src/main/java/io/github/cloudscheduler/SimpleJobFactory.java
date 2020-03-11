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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Very simple job factory implementation. Just use default constructor to create job instance.
 *
 * @author Wei Gao
 */
public class SimpleJobFactory implements JobFactory {

  private static final Logger logger = LoggerFactory.getLogger(SimpleJobFactory.class);

  private final ConcurrentMap<Class<?>, JobScheduleCalculator> calcuatorMap;

  public SimpleJobFactory() {
    calcuatorMap = new ConcurrentHashMap<>();
  }

  @Override
  public Job newJob(JobDefinition jobDefinition) throws Exception {
    Class<? extends Job> clazz = jobDefinition.getJobClass();
    Constructor<? extends Job> constructor = clazz.getConstructor();
    Job job = constructor.newInstance();
    logger.trace("Created job object instance");
    return job;
  }

  @Override
  public JobScheduleCalculator createJobScheduleCalculator(
      Class<? extends JobScheduleCalculator> calculatorClass) throws Throwable {
    try {
      return calcuatorMap.computeIfAbsent(calculatorClass, key -> {
        try {
          Constructor<? extends JobScheduleCalculator> constructor = calculatorClass
              .getConstructor();
          return constructor.newInstance();
        } catch (NoSuchMethodException
            | IllegalAccessException
            | InstantiationException
            | InvocationTargetException exp) {
          throw new CompletionException(exp);
        }
      });
    } catch (CompletionException exp) {
      throw exp.getCause();
    }
  }
}
