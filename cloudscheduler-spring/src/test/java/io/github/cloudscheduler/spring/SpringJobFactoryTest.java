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

import io.github.cloudscheduler.CloudSchedulerManager;
import io.github.cloudscheduler.JobFactory;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.service.JobService;
import io.github.cloudscheduler.service.JobServiceImpl;
import io.github.cloudscheduler.util.ZooKeeperUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.PropertySource;
import org.springframework.lang.Nullable;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Wei Gao
 */
public class SpringJobFactoryTest {
  private TestingServer zkTestServer;
  private ZooKeeper zooKeeper;
  private JobService jobService;
  private ExecutorService threadPool;

  @BeforeClass
  public void setup() {
    AtomicInteger threadCounter = new AtomicInteger(0);
    threadPool = Executors.newCachedThreadPool(r ->
        new Thread(r, "TestThread-" + threadCounter.incrementAndGet()));
  }

  @AfterClass
  public void teardown() {
    if (threadPool != null) {
      threadPool.shutdown();
    }
  }

  @Test
  public void testCreateNewJob() throws Throwable {
    zkTestServer = new TestingServer();
    zooKeeper = ZooKeeperUtils.connectToZooKeeper(zkTestServer.getConnectString(), Integer.MAX_VALUE).get();
    jobService = new JobServiceImpl(zooKeeper);
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    ctx.getEnvironment().getPropertySources()
        .addLast(new ZooKeeperPropertySource(zkTestServer.getConnectString()));
    ctx.register(Config.class);
    ctx.refresh();
    ctx.start();
    try {
      JobDefinition jobDefinition = JobDefinition.newBuilder(TestJob.class)
          .runNow()
          .build();
      jobService.saveJobDefinition(jobDefinition);
      TestJob job = ctx.getBean(TestJob.class);
      Assert.assertTrue(job.waitDown(1000L));
    } finally {
      ctx.close();
      try {
        zooKeeper.close();
      } catch (Throwable ignored) {
      }
      try {
        zkTestServer.close();
      } catch (Throwable ignored) {
      }
    }
  }

  @Configuration
  @ComponentScan("io.github.cloudscheduler.spring")
  static class Config {
    @Value("${myZooKeeperUrl}")
    private String zkUrl;

    @Bean
    JobFactory schedulerJobFactory() {
      return new AutowiringSpringBeanJobFactory();
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    CloudSchedulerManager cloudSchedulerManager() {
      return CloudSchedulerManager.newBuilder(zkUrl)
          .setThreadPool(Executors.newSingleThreadExecutor())
          .setJobFactory(schedulerJobFactory())
          .build();
    }
  }

  private static class ZooKeeperPropertySource extends PropertySource<String> {
    private final String zkUrl;

    private ZooKeeperPropertySource(String zkUrl) {
      super("zookeeper");
      this.zkUrl = zkUrl;
    }

    @Nullable
    @Override
    public Object getProperty(String s) {
      if ("myZooKeeperUrl".equals(s)) {
        return zkUrl;
      }
      return null;
    }
  }
}
