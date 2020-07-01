package io.github.cloudscheduler.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.github.cloudscheduler.EventType;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.codec.EntityCodec;
import io.github.cloudscheduler.codec.EntityCodecProvider;
import io.github.cloudscheduler.codec.EntityDecoder;
import io.github.cloudscheduler.codec.EntityEncoder;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobInstanceState;
import io.github.cloudscheduler.model.JobRunStatus;
import io.github.cloudscheduler.util.ZooKeeperUtils;
import io.github.cloudscheduler.util.ZooKeeperUtils.EntityHolder;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Tested;
import mockit.Verifications;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JobServiceImplTest {
  @Tested(availableDuringSetup = true)
  private JobServiceImpl cut;

  @Injectable private ZooKeeper zooKeeper;
  @Mocked private EntityCodecProvider codecProvider;

  @BeforeEach
  public void init() {
    new MockUp<EntityCodecProvider>() {
      @Mock
      public EntityCodecProvider getCodecProvider() {
        return codecProvider;
      }
    };
    cut.complete(null);
  }

  @Test
  public void testRegisterWorkerAsyncNodeExists(@Mocked Node node) {
    AtomicBoolean called = new AtomicBoolean(false);
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<Integer> exists(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(1);
      }

      @Mock
      public CompletableFuture<String> createEphemeralZnode(
          ZooKeeper zooKeeper, String dest, byte[] data) {
        called.set(true);
        return CompletableFuture.completedFuture(dest);
      }
    };
    UUID nodeId = UUID.randomUUID();
    new Expectations() {
      {
        node.getId();
        result = nodeId;
      }
    };
    assertThat(cut.registerWorkerAsync(node)).succeedsWithin(Duration.ofSeconds(5)).isSameAs(node);
    assertThat(called.get()).isFalse();
  }

  @Test
  public void testRegisterWorkerAsync(@Mocked Node node) {
    AtomicBoolean called = new AtomicBoolean(false);
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<Integer> exists(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(null);
      }

      @Mock
      public CompletableFuture<String> createEphemeralZnode(
          ZooKeeper zooKeeper, String dest, byte[] data) {
        called.set(true);
        return CompletableFuture.completedFuture(dest);
      }
    };
    UUID nodeId = UUID.randomUUID();
    new Expectations() {
      {
        node.getId();
        result = nodeId;
      }
    };
    assertThat(cut.registerWorkerAsync(node)).isDone().isCompletedWithValue(node);
    assertThat(called.get()).isTrue();
  }

  @Test
  public void testUnregisterWorkerAsyncNodeExists(@Mocked Node node) {
    AtomicBoolean called = new AtomicBoolean(false);
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<Integer> exists(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(1);
      }

      @Mock
      public CompletableFuture<Void> deleteIfExists(ZooKeeper zooKeeper, String dest) {
        called.set(true);
        return CompletableFuture.completedFuture(null);
      }
    };
    UUID nodeId = UUID.randomUUID();
    new Expectations() {
      {
        node.getId();
        result = nodeId;
      }
    };
    assertThat(cut.unregisterWorkerAsync(node))
        .succeedsWithin(Duration.ofSeconds(5))
        .isSameAs(node);
    assertThat(called.get()).isTrue();
  }

  @Test
  public void testUnregisterWorkerAsyncNodeNotExist(@Mocked Node node) {
    AtomicBoolean called = new AtomicBoolean(false);
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<Integer> exists(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(null);
      }

      @Mock
      public CompletableFuture<Void> deleteIfExists(ZooKeeper zooKeeper, String dest) {
        called.set(true);
        return CompletableFuture.completedFuture(null);
      }
    };
    UUID nodeId = UUID.randomUUID();
    new Expectations() {
      {
        node.getId();
        result = nodeId;
      }
    };
    assertThat(cut.unregisterWorkerAsync(node))
        .succeedsWithin(Duration.ofSeconds(5))
        .isSameAs(node);
    assertThat(called.get()).isFalse();
  }

  @Test
  public void testGetCurrentWorkerAsync() {
    List<UUID> ids = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());
    List<String> children = ids.stream().map(UUID::toString).collect(Collectors.toList());
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<List<String>> getChildren(
          ZooKeeper zooKeeper, String dest, Consumer<EventType> listener) {
        return CompletableFuture.completedFuture(children);
      }
    };
    assertThat(cut.getCurrentWorkersAsync()).isDone().isCompletedWithValue(ids);
  }

  @Test
  public void testSaveJobDefinitionAsync(
      @Mocked JobDefinition jobDef, @Mocked Transaction transaction) {
    UUID jobDefId = UUID.randomUUID();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<Integer> exists(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(null);
      }

      @Mock
      public <T> CompletableFuture<T> transactionalOperation(
          ZooKeeper zooKeeper, Function<Transaction, CompletableFuture<T>> function) {
        return function.apply(transaction);
      }
    };
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
      }
    };
    assertThat(cut.saveJobDefinitionAsync(jobDef)).isDone().isCompletedWithValue(jobDef);
  }

  @Test
  public void testSaveJobDefinitionAsyncExistError(@Mocked JobDefinition jobDef) {
    UUID jobDefId = UUID.randomUUID();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<Integer> exists(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(1);
      }
    };
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
      }
    };
    assertThat(cut.saveJobDefinitionAsync(jobDef))
        .hasFailedWithThrowableThat()
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testSaveJobDefinitionAsyncWithIOException(
      @Mocked EntityCodec<JobDefinition> jdCodec,
      @Mocked JobDefinition jobDef,
      @Mocked Transaction transaction) {
    UUID jobDefId = UUID.randomUUID();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<Integer> exists(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(null);
      }

      @Mock
      public <T> CompletableFuture<T> transactionalOperation(
          ZooKeeper zooKeeper, Function<Transaction, CompletableFuture<T>> function) {
        return function.apply(transaction);
      }
    };
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
        codecProvider.getEntityEncoder(JobDefinition.class);
        result = jdCodec;
        try {
          jdCodec.encode((JobDefinition) any);
          result = new IOException();
        } catch (IOException e) {
          // ignore it.
        }
      }
    };
    assertThat(cut.saveJobDefinitionAsync(jobDef))
        .hasFailedWithThrowableThat()
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(IOException.class);
  }

  @Test
  public void testGetJobDefinitionByIdAsync(@Mocked JobDefinition jobDef) {
    UUID id = UUID.randomUUID();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinition>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobDefinition> decoder) {
        return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobDef, 1));
      }
    };
    assertThat(cut.getJobDefinitionByIdAsync(id)).isDone().isCompletedWithValue(jobDef);
  }

  @Test
  public void testDeleteJobDefinitionAsync(
      @Mocked JobDefinition jobDef,
      @Mocked JobInstance jobInstance,
      @Mocked Transaction transaction) {
    UUID jobDefId = UUID.randomUUID();
    int version = 1;
    UUID jobInstanceId = UUID.randomUUID();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<Integer> exists(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(version);
      }

      @Mock
      public CompletableFuture<Void> transactionalOperation(
          ZooKeeper zooKeeper, Function<Transaction, CompletableFuture<Void>> function) {
        return function.apply(transaction);
      }

      @Mock
      public CompletableFuture<List<String>> getChildren(
          ZooKeeper zooKeeper, String dest, Consumer<EventType> listener) {
        return CompletableFuture.completedFuture(Arrays.asList(jobInstanceId.toString()));
      }

      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<Object>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<Object> decoder) {
        if (dest.endsWith(jobInstanceId.toString())) {
          return CompletableFuture.completedFuture(
              new ZooKeeperUtils.EntityHolder<>(jobInstance, version));
        }
        return null;
      }
    };
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
        jobInstance.getJobDefId();
        result = jobDefId;
        jobInstance.getId();
        result = jobInstanceId;
      }
    };
    assertThat(cut.deleteJobDefinitionAsync(jobDef)).isDone().isCompleted();
    new Verifications() {
      {
        List<String> path = new ArrayList<>();
        transaction.delete(withCapture(path), 1);
        Comparator<String> endWithSubstring = (o1, o2) -> o1.endsWith(o2) ? 0 : 1;
        assertThat(path)
            .hasSize(3)
            .usingElementComparator(endWithSubstring)
            .endsWith(
                jobInstanceId.toString(),
                jobDefId.toString() + "/" + JobServiceImpl.STATUS_PATH,
                jobDefId.toString());
      }
    };
  }

  @Test
  public void testDeleteJobDefinitionAsyncNotFound(
      @Mocked JobDefinition jobDef,
      @Mocked JobInstance jobInstance,
      @Mocked Transaction transaction) {
    UUID jobDefId = UUID.randomUUID();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<Integer> exists(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(null);
      }
    };
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
      }
    };
    assertThat(cut.deleteJobDefinitionAsync(jobDef)).isDone().isCompletedWithValue(null);
  }

  @Test
  public void testGetJobStatusByIdAsyncWithListener(
      @Mocked JobDefinitionStatus status, @Mocked Consumer<EventType> listener) {
    UUID jobDefId = UUID.randomUUID();
    AtomicReference<Consumer<EventType>> holder = new AtomicReference<>();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinitionStatus>> readEntity(
          ZooKeeper zooKeeper,
          String dest,
          EntityDecoder<JobDefinitionStatus> decoder,
          Consumer<EventType> listener) {
        holder.set(listener);
        return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(status, 1));
      }
    };
    assertThat(cut.getJobStatusByIdAsync(jobDefId, listener)).isDone().isCompletedWithValue(status);
    assertThat(holder).hasValue(listener);
  }

  @Test
  public void testGetJobStatusByIdAsyncWithOutListener() {
    UUID jobDefId = UUID.randomUUID();
    AtomicReference<Consumer<EventType>> holder = new AtomicReference<>();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinitionStatus>> readEntity(
          ZooKeeper zooKeeper,
          String dest,
          EntityDecoder<JobDefinitionStatus> decoder,
          Consumer<EventType> listener) {
        holder.set(listener);
        return CompletableFuture.completedFuture(null);
      }
    };
    assertThat(cut.getJobStatusByIdAsync(jobDefId)).isDone().isCompletedWithValue(null);
    assertThat(holder).hasValue(null);
  }

  @Test
  public void testGetJobInstanceByIdAsyncWithListener(
      @Mocked JobInstance jobIns, @Mocked Consumer<EventType> listener) {
    UUID jobInsId = UUID.randomUUID();
    AtomicReference<Consumer<EventType>> holder = new AtomicReference<>();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobInstance>> readEntity(
          ZooKeeper zooKeeper,
          String dest,
          EntityDecoder<JobInstance> decoder,
          Consumer<EventType> listener) {
        holder.set(listener);
        return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobIns, 1));
      }
    };
    assertThat(cut.getJobInstanceByIdAsync(jobInsId, listener))
        .isDone()
        .isCompletedWithValue(jobIns);
    assertThat(holder).hasValue(listener);
  }

  @Test
  public void testGetJobInstanceByIdAsyncWithOutListener() {
    UUID jobInsId = UUID.randomUUID();
    AtomicReference<Consumer<EventType>> holder = new AtomicReference<>();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobInstance>> readEntity(
          ZooKeeper zooKeeper,
          String dest,
          EntityDecoder<JobInstance> decoder,
          Consumer<EventType> listener) {
        holder.set(listener);
        return CompletableFuture.completedFuture(null);
      }
    };
    assertThat(cut.getJobInstanceByIdAsync(jobInsId)).isDone().isCompletedWithValue(null);
    assertThat(holder).hasValue(null);
  }

  @Test
  public void testDeleteJobInstanceAsync() {
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<Void> deleteIfExists(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(null);
      }
    };
    assertThat(cut.deleteJobInstanceAsync(UUID.randomUUID())).isDone().isCompleted();
  }

  @Test
  public void testListAllJobInstanceIdsAsync() {
    List<UUID> ids = Arrays.asList(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
    List<String> idStrs = ids.stream().map(UUID::toString).collect(Collectors.toList());
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<List<String>> getChildren(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(idStrs);
      }
    };
    assertThat(cut.listAllJobInstanceIdsAsync()).isDone().isCompletedWithValue(ids);
  }

  @Test
  public void testListAllJobInstancesAsync(
      @Mocked JobInstance jobIns1, @Mocked JobInstance jobIns2, @Mocked JobInstance jobIns3) {
    UUID jobIns1Id = UUID.randomUUID();
    UUID jobIns2Id = UUID.randomUUID();
    UUID jobIns3Id = UUID.randomUUID();
    UUID jobIns4Id = UUID.randomUUID();

    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<List<String>> getChildren(
          ZooKeeper zooKeeper, String dest, Consumer<EventType> listener) {
        return CompletableFuture.completedFuture(
            Arrays.asList(jobIns1Id, jobIns2Id, jobIns3Id, jobIns4Id).stream()
                .map(UUID::toString)
                .collect(Collectors.toList()));
      }

      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobInstance>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobInstance> decoder) {
        if (dest.endsWith(jobIns1Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobIns1, 1));
        } else if (dest.endsWith(jobIns2Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobIns2, 1));
        } else if (dest.endsWith(jobIns3Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobIns3, 1));
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }
    };

    assertThat(cut.listAllJobInstancesAsync())
        .isDone()
        .isCompletedWithValue(Arrays.asList(jobIns1, jobIns2, jobIns3));
  }

  @Test
  public void testListAllJobInstancesAsync(@Mocked Consumer<EventType> listener) {
    AtomicReference<Consumer<EventType>> holder = new AtomicReference<>();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<List<String>> getChildren(
          ZooKeeper zooKeeper, String dest, Consumer<EventType> listener) {
        holder.set(listener);
        return CompletableFuture.completedFuture(Collections.emptyList());
      }
    };

    assertThat(cut.listAllJobInstancesAsync(listener)).isDone();
    assertThat(holder).hasValue(listener);
  }

  @Test
  public void testGetJobInstancesByJobDefAsync(
      @Mocked JobDefinition jobDef1,
      @Mocked JobInstance jobIns1,
      @Mocked JobInstance jobIns2,
      @Mocked JobInstance jobIns3) {
    UUID jobDef1Id = UUID.randomUUID();
    UUID jobDef2Id = UUID.randomUUID();
    UUID jobIns1Id = UUID.randomUUID();
    UUID jobIns2Id = UUID.randomUUID();
    UUID jobIns3Id = UUID.randomUUID();
    UUID jobIns4Id = UUID.randomUUID();

    new Expectations() {
      {
        jobDef1.getId();
        result = jobDef1Id;
        jobIns1.getJobDefId();
        result = jobDef1Id;
        jobIns2.getJobDefId();
        result = jobDef2Id;
        jobIns3.getJobDefId();
        result = jobDef1Id;
      }
    };

    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<List<String>> getChildren(
          ZooKeeper zooKeeper, String dest, Consumer<EventType> listener) {
        return CompletableFuture.completedFuture(
            Arrays.asList(jobIns1Id, jobIns2Id, jobIns3Id, jobIns4Id).stream()
                .map(UUID::toString)
                .collect(Collectors.toList()));
      }

      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobInstance>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobInstance> decoder) {
        if (dest.endsWith(jobIns1Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobIns1, 1));
        } else if (dest.endsWith(jobIns2Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobIns2, 1));
        } else if (dest.endsWith(jobIns3Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobIns3, 1));
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }
    };

    assertThat(cut.getJobInstancesByJobDefAsync(jobDef1))
        .isDone()
        .isCompletedWithValue(Arrays.asList(jobIns1, jobIns3));
  }

  @Test
  public void testListAllJobDefinitionIdsAsync() {
    List<UUID> ids = Arrays.asList(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
    List<String> idStrs = ids.stream().map(UUID::toString).collect(Collectors.toList());
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<List<String>> getChildren(ZooKeeper zooKeeper, String dest) {
        return CompletableFuture.completedFuture(idStrs);
      }
    };
    assertThat(cut.listAllJobDefinitionIdsAsync()).isDone().isCompletedWithValue(ids);
  }

  @Test
  public void testListJobDefinitionsByNameAsync(
      @Mocked JobDefinition jobDef1, @Mocked JobDefinition jobDef2) {
    UUID jobDef1Id = UUID.randomUUID();
    UUID jobDef2Id = UUID.randomUUID();
    String jobDef1Name = "FirstJobDefinition";
    String jobDef2Name = "SecondJobDefinition";
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<List<String>> getChildren(
          ZooKeeper zooKeeper, String dest, Consumer<EventType> listener) {
        return CompletableFuture.completedFuture(
            Arrays.asList(jobDef1Id, jobDef2Id).stream()
                .map(UUID::toString)
                .collect(Collectors.toList()));
      }

      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinition>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobDefinition> decoder) {
        if (dest.endsWith(jobDef1Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobDef1, 1));
        } else if (dest.endsWith(jobDef2Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobDef2, 1));
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }
    };
    new Expectations() {
      {
        jobDef1.getName();
        result = jobDef1Name;
        jobDef2.getName();
        result = jobDef2Name;
      }
    };
    assertThat(cut.listJobDefinitionsByNameAsync(jobDef1Name))
        .isDone()
        .isCompletedWithValue(Arrays.asList(jobDef1));
  }

  @Test
  public void testListAllJobDefinitionsAsync(
      @Mocked JobDefinition jobDef1, @Mocked JobDefinition jobDef2) {
    UUID jobDef1Id = UUID.randomUUID();
    UUID jobDef2Id = UUID.randomUUID();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<List<String>> getChildren(
          ZooKeeper zooKeeper, String dest, Consumer<EventType> listener) {
        return CompletableFuture.completedFuture(
            Arrays.asList(jobDef1Id, jobDef2Id).stream()
                .map(UUID::toString)
                .collect(Collectors.toList()));
      }

      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinition>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobDefinition> decoder) {
        if (dest.endsWith(jobDef1Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobDef1, 1));
        } else if (dest.endsWith(jobDef2Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobDef2, 1));
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }
    };
    assertThat(cut.listAllJobDefinitionsAsync())
        .isDone()
        .isCompletedWithValue(Arrays.asList(jobDef1, jobDef2));
  }

  @Test
  public void testListAllJobDefinitionsAsync(@Mocked Consumer<EventType> listener) {
    AtomicReference<Consumer<EventType>> holder = new AtomicReference<>();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<List<String>> getChildren(
          ZooKeeper zooKeeper, String dest, Consumer<EventType> listener) {
        holder.set(listener);
        return CompletableFuture.completedFuture(Collections.emptyList());
      }
    };
    assertThat(cut.listAllJobDefinitionsAsync(listener)).isDone().isCompleted();
    assertThat(holder).hasValue(listener);
  }

  @Test
  public void testListJobDefinitionsWithStatusAsync(
      @Mocked JobDefinition jobDef1,
      @Mocked JobDefinition jobDef2,
      @Mocked JobDefinitionStatus status1,
      @Mocked JobDefinitionStatus status2) {
    UUID jobDef1Id = UUID.randomUUID();
    UUID jobDef2Id = UUID.randomUUID();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<List<String>> getChildren(
          ZooKeeper zooKeeper, String dest, Consumer<EventType> listener) {
        return CompletableFuture.completedFuture(
            Arrays.asList(jobDef1Id, jobDef2Id).stream()
                .map(UUID::toString)
                .collect(Collectors.toList()));
      }

      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinition>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobDefinition> decoder) {
        if (dest.endsWith(jobDef1Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobDef1, 1));
        } else if (dest.endsWith(jobDef2Id.toString())) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(jobDef2, 1));
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }

      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinitionStatus>> readEntity(
          ZooKeeper zooKeeper,
          String dest,
          EntityDecoder<JobDefinitionStatus> decoder,
          Consumer<EventType> listener) {
        if (dest.endsWith(jobDef1Id.toString() + "/" + JobServiceImpl.STATUS_PATH)) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(status1, 1));
        } else if (dest.endsWith(jobDef2Id.toString() + "/" + JobServiceImpl.STATUS_PATH)) {
          return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(status2, 1));
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }
    };
    new Expectations() {
      {
        jobDef1.getId();
        result = jobDef1Id;
        jobDef2.getId();
        result = jobDef2Id;
      }
    };
    CompletableFuture<Map<JobDefinition, JobDefinitionStatus>> future =
        cut.listJobDefinitionsWithStatusAsync();
    assertThat(future).isDone().isCompleted();
    assertThat(future.join())
        .extractingFromEntries(e -> e.getKey(), e -> e.getValue())
        .containsOnly(tuple(jobDef1, status1), tuple(jobDef2, status2));
  }

  @Test
  public void testScheduleJobInstanceAsync(
      @Mocked JobDefinition jobDef,
      @Mocked JobDefinitionStatus status,
      @Mocked Transaction transaction) {
    Map<UUID, JobInstanceState> jobInstanceStateMap = new HashMap<>();
    UUID jobDefId = UUID.randomUUID();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinitionStatus>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobDefinitionStatus> decoder) {
        return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(status, 1));
      }

      @Mock
      public CompletableFuture<JobInstance> transactionalOperation(
          ZooKeeper zooKeeper, Function<Transaction, CompletableFuture<JobInstance>> function) {
        return function.apply(transaction);
      }
    };
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
        status.getId();
        result = jobDefId;
        jobDef.isGlobal();
        result = false;
        status.getJobInstanceState();
        result = jobInstanceStateMap;
      }
    };
    assertThat(cut.scheduleJobInstanceAsync(jobDef)).isDone().isCompleted();
    new Verifications() {
      {
        transaction.create(
            anyString, (byte[]) any, ZooKeeperUtils.DEFAULT_ACL, CreateMode.PERSISTENT);
        transaction.setData(anyString, (byte[]) any, 1);
      }
    };
  }

  @Test
  public void testScheduleJobInstanceAsyncGlobal(
      @Mocked JobDefinition jobDef,
      @Mocked JobDefinitionStatus status,
      @Mocked Transaction transaction) {
    Map<UUID, JobInstanceState> jobInstanceStateMap = new HashMap<>();
    Map<UUID, JobRunStatus> jobRunStatusMap = new HashMap<>();
    UUID jobDefId = UUID.randomUUID();
    UUID node1Id = UUID.randomUUID();
    UUID node2Id = UUID.randomUUID();

    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinitionStatus>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobDefinitionStatus> decoder) {
        return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(status, 1));
      }

      @Mock
      public CompletableFuture<JobInstance> transactionalOperation(
          ZooKeeper zooKeeper, Function<Transaction, CompletableFuture<JobInstance>> function) {
        return function.apply(transaction);
      }

      @Mock
      public CompletableFuture<List<String>> getChildren(
          ZooKeeper zooKeeper, String dest, Consumer<EventType> listener) {
        return CompletableFuture.completedFuture(
            Arrays.asList(node1Id, node2Id).stream()
                .map(UUID::toString)
                .collect(Collectors.toList()));
      }
    };
    new MockUp<JobInstance>() {
      @Mock
      public Map<UUID, JobRunStatus> getRunStatus() {
        return jobRunStatusMap;
      }
    };
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
        status.getId();
        result = jobDefId;
        jobDef.isGlobal();
        result = true;
        status.getJobInstanceState();
        result = jobInstanceStateMap;
      }
    };
    assertThat(cut.scheduleJobInstanceAsync(jobDef)).isDone().isCompleted();
    assertThat(jobRunStatusMap).containsKeys(node1Id, node2Id);
    new Verifications() {
      {
        transaction.create(
            anyString, (byte[]) any, ZooKeeperUtils.DEFAULT_ACL, CreateMode.PERSISTENT);
        transaction.setData(anyString, (byte[]) any, 1);
      }
    };
  }

  @Test
  public void testScheduleJobInstanceAsyncJobDefinitionStatusNotFound(
      @Mocked JobDefinition jobDef) {
    Map<UUID, JobRunStatus> jobRunStatusMap = new HashMap<>();
    UUID jobDefId = UUID.randomUUID();

    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinitionStatus>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobDefinitionStatus> decoder) {
        return CompletableFuture.completedFuture(null);
      }
    };
    new MockUp<JobInstance>() {
      @Mock
      public Map<UUID, JobRunStatus> getRunStatus() {
        return jobRunStatusMap;
      }
    };
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
      }
    };
    assertThat(cut.scheduleJobInstanceAsync(jobDef))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testScheduleJobInstanceAsyncIOException(
      @Mocked JobDefinition jobDef,
      @Mocked JobDefinitionStatus status,
      @Mocked Transaction transaction,
      @Mocked EntityEncoder<JobInstance> jsEncoder) {
    UUID jobDefId = UUID.randomUUID();

    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<ZooKeeperUtils.EntityHolder<JobDefinitionStatus>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobDefinitionStatus> decoder) {
        return CompletableFuture.completedFuture(new ZooKeeperUtils.EntityHolder<>(status, 1));
      }

      @Mock
      public CompletableFuture<JobInstance> transactionalOperation(
          ZooKeeper zooKeeper, Function<Transaction, CompletableFuture<JobInstance>> function) {
        return function.apply(transaction);
      }
    };
    new Expectations() {
      {
        jobDef.getId();
        result = jobDefId;
        codecProvider.getEntityEncoder(JobInstance.class);
        result = jsEncoder;
        try {
          jsEncoder.encode((JobInstance) any);
          result = new IOException();
        } catch (IOException __) {
          // ignore
        }
        jobDef.isGlobal();
        result = false;
      }
    };
    assertThat(cut.scheduleJobInstanceAsync(jobDef))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(IOException.class);
  }

  @Test
  public void testStartProcessJobInstanceAsync(@Mocked JobInstance jobIns) {
    UUID jobInsId = UUID.randomUUID();
    UUID nodeId = UUID.randomUUID();
    Map<UUID, JobRunStatus> jobRunStatusMap = new HashMap<>();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<EntityHolder<JobInstance>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobInstance> decoder) {
        return CompletableFuture.completedFuture(new EntityHolder<>(jobIns, 1));
      }

      @Mock
      public CompletableFuture<Object> updateEntity(
          ZooKeeper zooKeeper,
          String dest,
          Object entity,
          EntityEncoder<JobInstance> encoder,
          int version) {
        return CompletableFuture.completedFuture(entity);
      }
    };
    new Expectations() {
      {
        jobIns.getRunStatus();
        result = jobRunStatusMap;
        jobIns.getId();
        result = jobInsId;
      }
    };

    assertThat(cut.startProcessJobInstanceAsync(jobInsId, nodeId))
        .isDone()
        .isCompletedWithValue(jobIns);
    assertThat(jobRunStatusMap)
        .containsKeys(nodeId)
        .extractingByKey(nodeId)
        .hasFieldOrPropertyWithValue("state", JobInstanceState.RUNNING);
  }

  @Test
  public void testStartProcessJobInstanceAsyncNotFound(@Mocked JobInstance jobIns) {
    UUID jobInsId = UUID.randomUUID();
    UUID nodeId = UUID.randomUUID();
    Map<UUID, JobRunStatus> jobRunStatusMap = new HashMap<>();
    new MockUp<ZooKeeperUtils>() {
      @Mock
      public CompletableFuture<EntityHolder<JobInstance>> readEntity(
          ZooKeeper zooKeeper, String dest, EntityDecoder<JobInstance> decoder) {
        return CompletableFuture.completedFuture(null);
      }
    };

    assertThat(cut.startProcessJobInstanceAsync(jobInsId, nodeId))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testCompleteJobInstanceAsync() {}
}
