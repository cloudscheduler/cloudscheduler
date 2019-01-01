# Cloud Scheduler
Cloud scheduler is a zookeeper based scheduler for cloud environment. It heavily
rely on CompletableFuture introduced in JDK 8. But with minimum dependencies.
## Design goal
* Minimum Dependencies
Simple Cloud Scheduler been split into multiple projects in order to minimize
third party dependencies.
    * cloudscheduler-api:  Defined APIs, do not depend on any third party library.
    * cloudscheduler-codec-json: Jackson based entity codec implementations. Depends on
        * cloudscheduler-api
        * jackson-core
        * jackson-databind
        * jackson-annotations
        * jackson-module-parameter-names
        * jackson-datatype-jdk8
        * jackson-datatype-jsr310
    * cloudscheduler-codec-protobuf: Protostuff based entity codec implementations. Depends on
        * cloudscheduler-api
        * protostuff-api
        * protostuff-core
        * protostuff-runtime
        * protostuff-collectionschema
    * cloudscheduler-core: Implementation of cloud scheduler. Depends on
        * cloudscheduler-api
        * slf-api
        * zookeeper
    * cloudscheduler-spring: Spring application context based job factory. Used to
integrate with spring framework. Depends on:
        * cloudscheduler-api
        * spring-context.jar
* Thread module
    * Use CompletableFuture feature, no blocking logic.
    * Job run in user's own thread pool.
    * Minimum threads in simple cloud scheduler.

## How to use
### Dependencies
You will need at least:
* `cloudscheduler-api`
* `cloudscheduler-core`
* One of codec implementation. Default is: `cloudscheduler-codec-json`

If you want to use it in spring environment, you will also need
* `cloudscheduler-spring`: JobFactory implementation that will get Job implementation
spring beans

### Standalone application
#### Dependency definition:
Maven
```xml
<dependencies>
    <dependency>
        <groupId>io.github.cloudscheduler</groupId>
        <artifactId>cloudscheduler-core</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>io.github.cloudscheduler</groupId>
        <artifactId>cloudscheduler-codec-json</artifactId>
        <version>${version}</version>
    </dependency>
</dependencies>
```
#### Initial a CloudSchedulerManager
``` java
// Create a thread pool you want to use to execute your job.
ExecutorService threadPool = Executors.newCacheThreadPool();
// Create a cloud scheduler manager
CloudSchedulerManager manager = CloudSchedulerManager.newBuilder(zkUrl)
    .setZkTimeout(zkTimeout)
    .setThreadPool(threadPool)
    .build();
// Start cloud scheduler manager
manager.start();
```
#### Create Scheduler
End user will use API on Scheduler to schedule job
```java
// Create a zookeeper
ZooKeeper zkClient = new ZooKeeper(zkUrl, zkTimeout, watcher);
// Create a scheduler from zookeeper
Scheduler scheduler = new SchedulerImpl(zkClient);
```
#### Create a job class
```java
public class MyJob extends Job {
    public void execute(JobExecutionContext ctx) {
        // Your business logic here
    }
}
```
#### Schedule the job
```java
// Run job now without repeat
scheduler.runNow(MyJob.class);

// Run job at a given time without repeat
Instant start = Instant.now().plus(Duration.ofMinutes(5));
scheduler.runOnce(MyJob.class, start)
```
#### Shutdown CloudSchedulerManager after everything done
```java
manager.shutdown();
```
### Spring application
#### Dependency definition:
Maven
```xml
<dependencies>
    <dependency>
        <groupId>io.github.cloudscheduler</groupId>
        <artifactId>cloudscheduler-core</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>io.github.cloudscheduler</groupId>
        <artifactId>cloudscheduler-codec-json</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>io.github.cloudscheduler</groupId>
        <artifactId>cloudscheduler-spring</artifactId>
        <version>${version}</version>
    </dependency>
</dependencies>
```
#### Define beans
```xml
<bean id="csmBuilder" class="io.github.cloudscheduler.CloudSchedulerManager$Builder"
    factory-method="newBuilder">
    <constructor-arg>zookeeper1:2181,zookeeeper2:2181,zookeeper3:2181/cloudscheduler</constructor-arg>
    <property name="zkTimeout">5000</property>
    <property name="threadPool"><ref bean="MyThreadPool" /></property>
</bean>
<bean class="io.github.cloudscheduler.CloudSchedulerManager" factory-bean="csmBuilder" factory-method="build"
    init-method="start" destroy-method="shutdown" />

<bean class="io.github.cloudscheduler.SchedulerImpl">
    <constructor-arg index="1"><ref bean="zookeeper"></ref></constructor-arg>
</bean>
```
#### Wire beans
```java
@Autowired
private Scheduler scheduler;

...
scheduler.runNow(MyJob.class);
```

### More complicated job
* Start after 1 hour, repeat every 5 hour for 10 times
```java
JobDefinition jobDef = JobDefinition.newBuilder(MyJob.class)
    .initialDelay(Duration.ofHours(1))
    .fixedRate(Duration.ofHours(5))
    .repeat(10)
    .build();
scheduler.schedule(jobDef);
```
* Start after 1 hour, repeat after 5 hour of previous job done for 3 times
```java
JobDefinition jobDef = JobDefinition.newBuilder(MyJob.class)
    .initialDelay(Duration.ofHours(1))
    .fixedDelay(Duration.ofHours(5))
    .repeat(3)
    .build();
scheduler.schedule(jobDef);
```
* Use crontab format to define job (run at 12 every day). Allow next job start even previous job not done yet
```java
JobDefinition jobDef = JobDefinition.newBuilder(MyJob.class)
    .cron("0 12 * * *")
    .allowDupInstances()
    .build();
scheduler.schedule(jobDef);
```
* Create an one time job, but run on all worker nodes
```java
JobDefinition jobDef = JobDefinition.newBuilder(MyJob.class)
    .global()
    .build();
scheduler.schedule(jobDef);
```
