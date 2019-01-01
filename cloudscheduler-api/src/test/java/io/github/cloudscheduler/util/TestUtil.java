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

package io.github.cloudscheduler.util;

import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobRunStatus;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.testng.Assert;

public abstract class TestUtil {
  public static void jobDefinitionDeepEqual(JobDefinition actual, JobDefinition expected) {
    if (actual == null && expected == null) {
      return;
    }
    Assert.assertEquals(actual, expected);
    Assert.assertEquals(actual.getId(), expected.getId());
    Assert.assertEquals(actual.getJobClass(), expected.getJobClass());
    if (actual.getData() != null && expected.getData() != null) {
      Assert.assertEqualsDeep(actual.getData(), expected.getData());
    } else if (actual.getData() == null && expected.getData() != null && !expected.getData().isEmpty()) {
      Assert.fail("JobDefinition data doesn't match.");
    } else if (expected.getData() == null && actual.getData() != null && !actual.getData().isEmpty()) {
      Assert.fail("JobDefinition data doesn't match.");
    }
    Assert.assertEquals(actual.getName(), expected.getName());
    Assert.assertEquals(actual.getMode(), expected.getMode());
    Assert.assertEquals(actual.getCron(), expected.getCron());
    Assert.assertEquals(actual.getStartTime(), expected.getStartTime());
    Assert.assertEquals(actual.getRate(), expected.getRate());
    Assert.assertEquals(actual.getDelay(), expected.getDelay());
    Assert.assertEquals(actual.getRepeat(), expected.getRepeat());
    Assert.assertEquals(actual.getEndTime(), expected.getEndTime());
    Assert.assertEquals(actual.isGlobal(), expected.isGlobal());
    Assert.assertEquals(actual.isAllowDupInstances(), expected.isAllowDupInstances());
  }

  public static void jobDefinitionStatusDeepEqual(JobDefinitionStatus actual, JobDefinitionStatus expected) {
    if (actual == null && expected == null) {
      return;
    }
    Assert.assertEquals(actual, expected);
    Assert.assertEquals(actual.getId(), expected.getId());
    Assert.assertEquals(actual.getRunCount(), expected.getRunCount());
    Assert.assertEquals(actual.getLastScheduleTime(), expected.getLastScheduleTime());
    Assert.assertEquals(actual.getLastCompleteTime(), expected.getLastCompleteTime());
    Assert.assertEquals(actual.getState(), expected.getState());
    Assert.assertEqualsDeep(actual.getJobInstanceState(), expected.getJobInstanceState());
  }

  public static void jobInstanceDeepEqual(JobInstance actual, JobInstance expected) {
    if (actual == null && expected == null) {
      return;
    }
    Assert.assertEquals(actual.getId(), expected.getId());
    Assert.assertEquals(actual.getJobDefId(), expected.getJobDefId());
    Assert.assertEquals(actual.getScheduledTime(), expected.getScheduledTime());
    Assert.assertEquals(actual.getJobState(), expected.getJobState());
    Map<UUID, JobRunStatus> actualRunStatus = actual.getRunStatus();
    Map<UUID, JobRunStatus> expectedRunStatus = expected.getRunStatus();
    if (actualRunStatus == expectedRunStatus) {
      return;
    }

    if (actualRunStatus == null || expectedRunStatus == null) {
      Assert.fail("Maps not equal: expected: " + expectedRunStatus + " and actual: " + actualRunStatus);
    }

    if (actualRunStatus.size() != expectedRunStatus.size()) {
      Assert.fail("Maps do not have the same size:" + actualRunStatus.size() + " != " + expectedRunStatus.size());
    }

    Set<Map.Entry<UUID, JobRunStatus>> entrySet = actualRunStatus.entrySet();
    for (Map.Entry<UUID, JobRunStatus> anEntrySet : entrySet) {
      Map.Entry<UUID, JobRunStatus> entry = anEntrySet;
      UUID key = entry.getKey();
      JobRunStatus value = entry.getValue();
      JobRunStatus expectedValue = expectedRunStatus.get(key);
      String assertMessage = "Maps do not match for key:"
          + key + " actual:" + value + " expected:" + expectedValue;
      jobRunStatusDeepEqual(value, expectedValue, assertMessage);
    }
  }

  public static void jobRunStatusDeepEqual(JobRunStatus actual, JobRunStatus expected) {
    jobRunStatusDeepEqual(actual, expected, null);
  }

  public static void jobRunStatusDeepEqual(JobRunStatus actual, JobRunStatus expected, String assertMessage) {
    if (actual == null && expected == null) {
      return;
    }
    Assert.assertEquals(actual.getNodeId(), expected.getNodeId(), assertMessage);
    Assert.assertEquals(actual.getStartTime(), expected.getStartTime(), assertMessage);
    Assert.assertEquals(actual.getFinishTime(), expected.getFinishTime(), assertMessage);
    Assert.assertEquals(actual.getState(), expected.getState(), assertMessage);
  }
}
