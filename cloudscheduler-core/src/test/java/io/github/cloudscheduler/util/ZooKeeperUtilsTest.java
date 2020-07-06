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

package io.github.cloudscheduler.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cloudscheduler.EventType;
import io.github.cloudscheduler.codec.EntityDecoder;
import io.github.cloudscheduler.codec.EntityEncoder;
import io.github.cloudscheduler.util.ZooKeeperUtils.EntityHolder;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperUtilsTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperUtilsTest.class);
  @Mocked private ZooKeeper zooKeeper;
  @Mocked private WatchedEvent event;
  private Watcher watcher;

  @Test
  public void testConnectToZooKeeper() {
    new MockUp<ZooKeeper>() {
      @Mock
      public void $init(String connectString, int sessionTimeout, Watcher eventWatcher)
          throws IOException {
        watcher = eventWatcher;
        watcher.process(event);
      }
    };
    new Expectations() {
      {
        event.getState();
        result = KeeperState.SyncConnected;
      }
    };
    assertThat(ZooKeeperUtils.connectToZooKeeper("localhost", 100)).isDone().isCompleted();
  }

  @Test
  public void testConnectToZooKeeperWithListener(@Mocked Consumer<EventType> disconnectListener) {
    new MockUp<ZooKeeper>() {
      @Mock
      public void $init(String connectString, int sessionTimeout, Watcher eventWatcher)
          throws IOException {
        watcher = eventWatcher;
        watcher.process(event);
      }
    };
    new Expectations() {
      {
        event.getState();
        returns(KeeperState.SyncConnected, KeeperState.Disconnected);
      }
    };
    assertThat(ZooKeeperUtils.connectToZooKeeper("localhsot", 100, disconnectListener))
        .isDone()
        .isCompleted();
    watcher.process(event);
    new Verifications() {
      {
        disconnectListener.accept((EventType) any);
      }
    };
  }

  @Test
  public void testCreateZNodes(@Mocked Transaction transaction) {
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, null);
      }
    };
    new MockUp<Transaction>() {
      @Mock
      public void commit(MultiCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), null, ctx, Collections.emptyList());
      }
    };
    new Expectations() {
      {
        zooKeeper.transaction();
        result = transaction;
      }
    };
    String[] nodes = {"root", "folder1", "folder2", "item"};
    String path = String.join("/", nodes);
    assertThat(ZooKeeperUtils.createZnodes(zooKeeper, path))
        .isDone()
        .isCompletedWithValue("/" + path);
  }

  @Test
  public void testCreateZNodesNotExist(@Mocked Transaction transaction) {
    String[] nodes = {"root", "folder1", "folder2", "item"};
    String fullPath = String.join("/", nodes);
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        if (path.startsWith("/root/folder1/")) {
          cb.processResult(Code.NONODE.intValue(), path, ctx, null);
        } else {
          cb.processResult(Code.OK.intValue(), path, ctx, null);
        }
      }
    };
    new MockUp<Transaction>() {
      @Mock
      public void commit(MultiCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), null, ctx, Collections.emptyList());
      }
    };
    new Expectations() {
      {
        zooKeeper.transaction();
        result = transaction;
      }
    };
    assertThat(ZooKeeperUtils.createZnodes(zooKeeper, fullPath))
        .isDone()
        .isCompletedWithValue("/" + fullPath);
  }

  @Test
  public void testCreateZNodesTransactionError(@Mocked Transaction transaction) {
    String[] nodes = {"root", "folder1", "folder2", "item"};
    String fullPath = String.join("/", nodes);
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        if (path.startsWith("/root/folder1/")) {
          cb.processResult(Code.NONODE.intValue(), path, ctx, null);
        } else {
          cb.processResult(Code.OK.intValue(), path, ctx, null);
        }
      }
    };
    new MockUp<Transaction>() {
      @Mock
      public void commit(MultiCallback cb, Object ctx) {
        cb.processResult(Code.NOAUTH.intValue(), null, ctx, Collections.emptyList());
      }
    };
    new Expectations() {
      {
        zooKeeper.transaction();
        result = transaction;
      }
    };
    assertThat(ZooKeeperUtils.createZnodes(zooKeeper, fullPath))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testCreateZNodesExistError(@Mocked Transaction transaction) {
    String[] nodes = {"root", "folder1", "folder2", "item"};
    String fullPath = String.join("/", nodes);
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        if (path.startsWith("/root/folder1/")) {
          cb.processResult(Code.NOAUTH.intValue(), path, ctx, null);
        } else {
          cb.processResult(Code.OK.intValue(), path, ctx, null);
        }
      }
    };
    new Expectations() {
      {
        zooKeeper.transaction();
        result = transaction;
      }
    };
    assertThat(ZooKeeperUtils.createZnodes(zooKeeper, fullPath))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testCreateZNode() {
    new MockUp<ZooKeeper>() {
      @Mock
      public void create(
          final String path,
          byte data[],
          List<ACL> acl,
          CreateMode createMode,
          StringCallback cb,
          Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, path);
      }
    };
    String path = "/root";
    assertThat(ZooKeeperUtils.createZnode(zooKeeper, path, CreateMode.PERSISTENT, null))
        .isDone()
        .isCompletedWithValue(path);
  }

  @Test
  public void testCreateZNodeWithError() {
    new MockUp<ZooKeeper>() {
      @Mock
      public void create(
          final String path,
          byte data[],
          List<ACL> acl,
          CreateMode createMode,
          StringCallback cb,
          Object ctx) {
        cb.processResult(Code.NOAUTH.intValue(), path, ctx, path);
      }
    };
    String path = "/root";
    assertThat(ZooKeeperUtils.createZnode(zooKeeper, path, CreateMode.PERSISTENT, null))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testGetChildren() {
    String path = "SomeFolder";
    List<String> children = Arrays.asList("item1", "item2", "item3");
    new MockUp<ZooKeeper>() {
      @Mock
      public void getChildren(
          final String path, Watcher watcher, Children2Callback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, children, null);
      }
    };
    assertThat(ZooKeeperUtils.getChildren(zooKeeper, path, null))
        .isDone()
        .isCompletedWithValue(children);
  }

  @Test
  public void testGetChildrenWithException() {
    String path = "SomeFolder";
    new MockUp<ZooKeeper>() {
      @Mock
      public void getChildren(
          final String path, Watcher watcher, Children2Callback cb, Object ctx) {
        cb.processResult(Code.NONODE.intValue(), path, ctx, null, null);
      }
    };
    assertThat(ZooKeeperUtils.getChildren(zooKeeper, path, null))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testGetChildrenWithListenerChildrenChanged() {
    String path = "SomeFolder";
    List<String> children = Arrays.asList("item1", "item2", "item3");
    AtomicReference<Watcher> watcherHolder = new AtomicReference<>();
    new MockUp<ZooKeeper>() {
      @Mock
      public void getChildren(
          final String path, Watcher watcher, Children2Callback cb, Object ctx) {
        watcherHolder.set(watcher);
        cb.processResult(Code.OK.intValue(), path, ctx, children, null);
      }
    };
    new Expectations() {
      {
        event.getType();
        result = Event.EventType.NodeChildrenChanged;
      }
    };
    AtomicReference<EventType> eventHolder = new AtomicReference<>();
    assertThat(ZooKeeperUtils.getChildren(zooKeeper, path, event -> eventHolder.set(event)))
        .isDone()
        .isCompletedWithValue(children);
    assertThat(watcherHolder).isNotNull();
    watcherHolder.get().process(event);
    assertThat(eventHolder).isNotNull().hasValue(EventType.CHILD_CHANGED);
  }

  @Test
  public void testGetChildrenWithListenerEntityChanged() {
    String path = "SomeFolder";
    List<String> children = Arrays.asList("item1", "item2", "item3");
    AtomicReference<Watcher> watcherHolder = new AtomicReference<>();
    new MockUp<ZooKeeper>() {
      @Mock
      public void getChildren(
          final String path, Watcher watcher, Children2Callback cb, Object ctx) {
        watcherHolder.set(watcher);
        cb.processResult(Code.OK.intValue(), path, ctx, children, null);
      }
    };
    new Expectations() {
      {
        event.getType();
        result = Event.EventType.NodeDeleted;
      }
    };
    AtomicReference<EventType> eventHolder = new AtomicReference<>();
    assertThat(ZooKeeperUtils.getChildren(zooKeeper, path, event -> eventHolder.set(event)))
        .isDone()
        .isCompletedWithValue(children);
    assertThat(watcherHolder).isNotNull();
    watcherHolder.get().process(event);
    assertThat(eventHolder).isNotNull().hasValue(EventType.ENTITY_UPDATED);
  }

  @Test
  public void testWaitTillDoneNodeNotExist() {
    String path = "/root/somenode";
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(final String path, Watcher watcher, StatCallback cb, Object ctx) {
        cb.processResult(Code.NONODE.intValue(), path, ctx, null);
      }
    };
    assertThat(ZooKeeperUtils.waitTillGone(zooKeeper, path)).isDone().isCompleted();
  }

  @Test
  public void testWaitTillDoneNodeDeleted() {
    String path = "/root/somenode";
    AtomicReference<Watcher> watcherHolder = new AtomicReference<>();
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(final String path, Watcher watcher, StatCallback cb, Object ctx) {
        watcherHolder.set(watcher);
        cb.processResult(Code.OK.intValue(), path, ctx, null);
      }
    };
    new Expectations() {
      {
        event.getType();
        result = Event.EventType.NodeDeleted;
      }
    };
    CompletableFuture<Void> future = ZooKeeperUtils.waitTillGone(zooKeeper, path);
    assertThat(watcherHolder.get()).isNotNull();
    assertThat(future).isNotDone();
    watcherHolder.get().process(event);
    assertThat(future).isDone().isCompleted();
  }

  @Test
  public void testWaitTillDoneWithException() {
    String path = "/root/somenode";
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(final String path, Watcher watcher, StatCallback cb, Object ctx) {
        cb.processResult(Code.INVALIDACL.intValue(), path, ctx, null);
      }
    };
    assertThat(ZooKeeperUtils.waitTillGone(zooKeeper, path))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testDeleteIfExistsNotExist() {
    String path = "/root/somenode";
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(final String path, boolean watch, StatCallback cb, Object ctx) {
        cb.processResult(Code.NONODE.intValue(), path, ctx, null);
      }
    };
    assertThat(ZooKeeperUtils.deleteIfExists(zooKeeper, path)).isDone().isCompleted();
  }

  @Test
  public void testDeleteIfExistsCheckExistError() {
    String path = "/root/somenode";
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(final String path, boolean watch, StatCallback cb, Object ctx) {
        cb.processResult(Code.BADARGUMENTS.intValue(), path, ctx, null);
      }
    };
    assertThat(ZooKeeperUtils.deleteIfExists(zooKeeper, path))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testDeleteIfExistsDeleted(@Mocked Stat stat) {
    String path = "/root/somenode";
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(final String path, boolean watch, StatCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, stat);
      }

      @Mock
      public void delete(final String path, int version, VoidCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx);
      }
    };
    new Expectations() {
      {
        stat.getVersion();
        result = 1;
      }
    };
    assertThat(ZooKeeperUtils.deleteIfExists(zooKeeper, path)).isDone().isCompleted();
  }

  @Test
  public void testDeleteIfExistsDeletedError(@Mocked Stat stat) {
    String path = "/root/somenode";
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(final String path, boolean watch, StatCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, stat);
      }

      @Mock
      public void delete(final String path, int version, VoidCallback cb, Object ctx) {
        cb.processResult(Code.BADVERSION.intValue(), path, ctx);
      }
    };
    new Expectations() {
      {
        stat.getVersion();
        result = 1;
      }
    };
    assertThat(ZooKeeperUtils.deleteIfExists(zooKeeper, path))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testDeleteIfExistsRecursiveFalse() {
    String path = "/root/somenode";
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(final String path, boolean watch, StatCallback cb, Object ctx) {
        cb.processResult(Code.NONODE.intValue(), path, ctx, null);
      }
    };
    assertThat(ZooKeeperUtils.deleteIfExists(zooKeeper, path, false)).isDone().isCompleted();
  }

  @Test
  public void testDeleteIfExistsRecursiveTrue() {
    String rootPath = "/root/somenode";
    List<String> children = Arrays.asList("item1", "item2", "item3");
    new MockUp<ZooKeeper>() {
      @Mock
      public void getChildren(
          final String path, Watcher watcher, Children2Callback cb, Object ctx) {
        if (path.equals(rootPath)) {
          cb.processResult(Code.OK.intValue(), path, ctx, children, null);
        } else {
          cb.processResult(Code.OK.intValue(), path, ctx, Collections.emptyList(), null);
        }
      }

      @Mock
      public void exists(final String path, boolean watch, StatCallback cb, Object ctx) {
        cb.processResult(Code.NONODE.intValue(), path, ctx, null);
      }
    };
    assertThat(ZooKeeperUtils.deleteIfExists(zooKeeper, rootPath, true)).isDone().isCompleted();
  }

  @Test
  public void testReadEntity(@Mocked EntityDecoder<Object> decoder, @Mocked Stat stat)
      throws IOException {
    Object entity = new Object();
    new MockUp<ZooKeeper>() {
      @Mock
      public void getData(final String path, Watcher watcher, DataCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, new byte[0], stat);
      }
    };
    new Expectations() {
      {
        decoder.decode((byte[]) any);
        result = entity;
        stat.getVersion();
        result = 11;
      }
    };
    CompletableFuture<EntityHolder<Object>> future =
        ZooKeeperUtils.readEntity(zooKeeper, "", decoder);
    assertThat(future).isDone().isCompleted();
    assertThat(future.join())
        .hasFieldOrPropertyWithValue("entity", entity)
        .hasFieldOrPropertyWithValue("version", 11);
  }

  @Test
  public void testReadEntityNull(@Mocked EntityDecoder<Object> decoder) throws IOException {
    Object entity = new Object();
    new MockUp<ZooKeeper>() {
      @Mock
      public void getData(final String path, Watcher watcher, DataCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, null, null);
      }
    };
    CompletableFuture<EntityHolder<Object>> future =
        ZooKeeperUtils.readEntity(zooKeeper, "", decoder);
    assertThat(future).isDone().isCompleted();
    assertThat(future.join()).isNull();
  }

  @Test
  public void testReadEntityDecodeNotExist(@Mocked EntityDecoder<Object> decoder)
      throws IOException {
    Object entity = new Object();
    new MockUp<ZooKeeper>() {
      @Mock
      public void getData(final String path, Watcher watcher, DataCallback cb, Object ctx) {
        cb.processResult(Code.NONODE.intValue(), path, ctx, new byte[0], null);
      }
    };
    CompletableFuture<EntityHolder<Object>> future =
        ZooKeeperUtils.readEntity(zooKeeper, "", decoder);
    assertThat(future).isDone().isCompleted();
    assertThat(future.join()).isNull();
  }

  @Test
  public void testReadEntityDecodeError(@Mocked EntityDecoder<Object> decoder) throws IOException {
    Object entity = new Object();
    new MockUp<ZooKeeper>() {
      @Mock
      public void getData(final String path, Watcher watcher, DataCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, new byte[0], null);
      }
    };
    new Expectations() {
      {
        decoder.decode((byte[]) any);
        result = new IOException();
      }
    };
    assertThat(ZooKeeperUtils.readEntity(zooKeeper, "", decoder))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(IOException.class);
  }

  @Test
  public void testReadEntityOtherError(@Mocked EntityDecoder<Object> decoder) throws IOException {
    Object entity = new Object();
    new MockUp<ZooKeeper>() {
      @Mock
      public void getData(final String path, Watcher watcher, DataCallback cb, Object ctx) {
        cb.processResult(Code.NOAUTH.intValue(), path, ctx, new byte[0], null);
      }
    };
    assertThat(ZooKeeperUtils.readEntity(zooKeeper, "", decoder))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testReadEntityWithListenerDataChanged(
      @Mocked EntityDecoder<Object> decoder,
      @Mocked Stat stat,
      @Mocked Consumer<EventType> listener)
      throws IOException {
    Object entity = new Object();
    AtomicReference<Watcher> watcherHolder = new AtomicReference<>();
    new MockUp<ZooKeeper>() {
      @Mock
      public void getData(final String path, Watcher watcher, DataCallback cb, Object ctx) {
        watcherHolder.set(watcher);
        cb.processResult(Code.OK.intValue(), path, ctx, new byte[0], stat);
      }
    };
    new Expectations() {
      {
        decoder.decode((byte[]) any);
        result = entity;
        stat.getVersion();
        result = 1;
        event.getType();
        result = Event.EventType.NodeDataChanged;
      }
    };
    CompletableFuture<EntityHolder<Object>> future =
        ZooKeeperUtils.readEntity(zooKeeper, "", decoder, listener);
    assertThat(future).isDone().isCompleted();
    assertThat(future.join())
        .hasFieldOrPropertyWithValue("entity", entity)
        .hasFieldOrPropertyWithValue("version", 1);
    assertThat(watcherHolder.get()).isNotNull();
    watcherHolder.get().process(event);
    new Verifications() {
      {
        EventType type;
        listener.accept(type = withCapture());
        assertThat(type).isEqualTo(EventType.ENTITY_UPDATED);
      }
    };
  }

  @Test
  public void testReadEntityWithListenerNodeDeleted(
      @Mocked EntityDecoder<Object> decoder,
      @Mocked Stat stat,
      @Mocked Consumer<EventType> listener)
      throws IOException {
    Object entity = new Object();
    AtomicReference<Watcher> watcherHolder = new AtomicReference<>();
    new MockUp<ZooKeeper>() {
      @Mock
      public void getData(final String path, Watcher watcher, DataCallback cb, Object ctx) {
        watcherHolder.set(watcher);
        cb.processResult(Code.OK.intValue(), path, ctx, new byte[0], stat);
      }
    };
    new Expectations() {
      {
        decoder.decode((byte[]) any);
        result = entity;
        stat.getVersion();
        result = 1;
        event.getType();
        result = Event.EventType.NodeDeleted;
      }
    };
    CompletableFuture<EntityHolder<Object>> future =
        ZooKeeperUtils.readEntity(zooKeeper, "", decoder, listener);
    assertThat(future).isDone().isCompleted();
    assertThat(future.join())
        .hasFieldOrPropertyWithValue("entity", entity)
        .hasFieldOrPropertyWithValue("version", 1);
    assertThat(watcherHolder.get()).isNotNull();
    watcherHolder.get().process(event);
    new Verifications() {
      {
        EventType type;
        listener.accept(type = withCapture());
        assertThat(type).isEqualTo(EventType.ENTITY_DELETED);
      }
    };
  }

  @Test
  public void testExistsNotExist() {
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        cb.processResult(Code.NONODE.intValue(), path, ctx, null);
      }
    };
    assertThat(ZooKeeperUtils.exists(zooKeeper, "")).isDone().isCompletedWithValue(null);
  }

  @Test
  public void testExists(@Mocked Stat stat) {
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, stat);
      }
    };
    new Expectations() {
      {
        stat.getVersion();
        result = 2;
      }
    };
    assertThat(ZooKeeperUtils.exists(zooKeeper, "")).isDone().isCompletedWithValue(2);
  }

  @Test
  public void testExistsError() {
    new MockUp<ZooKeeper>() {
      @Mock
      public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        cb.processResult(Code.NOAUTH.intValue(), path, ctx, null);
      }
    };
    assertThat(ZooKeeperUtils.exists(zooKeeper, ""))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testCreatePersistentZnode() {
    testCreateZnodeMode(
        () -> ZooKeeperUtils.createPersistentZnode(zooKeeper, "/root", null),
        "/root",
        CreateMode.PERSISTENT);
  }

  @Test
  public void testCreateEphemeralZnode() {
    testCreateZnodeMode(
        () -> ZooKeeperUtils.createEphemeralZnode(zooKeeper, "/root", null),
        "/root",
        CreateMode.EPHEMERAL);
  }

  private void testCreateZnodeMode(
      Supplier<CompletableFuture<String>> supplier, String path, CreateMode mode) {
    AtomicReference<CreateMode> createModeHolder = new AtomicReference<>();
    new MockUp<ZooKeeper>() {
      @Mock
      public void create(
          final String path,
          byte data[],
          List<ACL> acl,
          CreateMode createMode,
          StringCallback cb,
          Object ctx) {
        createModeHolder.set(mode);
        cb.processResult(Code.OK.intValue(), path, ctx, path);
      }
    };
    assertThat(supplier.get()).isDone().isCompletedWithValue(path);
    assertThat(createModeHolder.get()).isNotNull().isEqualTo(mode);
  }

  @Test
  public void testUpdateEntity(@Mocked EntityEncoder<Object> encoder) throws IOException {
    Object entity = new Object();
    String path = "/somewhere";
    byte[] data = new byte[0];
    new MockUp<ZooKeeper>() {
      @Mock
      public void setData(
          final String path, byte data[], int version, StatCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, null);
      }
    };
    new Expectations() {
      {
        encoder.encode(entity);
        result = data;
      }
    };
    assertThat(ZooKeeperUtils.updateEntity(zooKeeper, path, entity, encoder, 1))
        .isDone()
        .isCompletedWithValue(entity);
  }

  @Test
  public void testUpdateEntityEncodeError(@Mocked EntityEncoder<Object> encoder)
      throws IOException {
    Object entity = new Object();
    String path = "/somewhere";
    byte[] data = new byte[0];
    new Expectations() {
      {
        encoder.encode(entity);
        result = new IOException();
      }
    };
    assertThat(ZooKeeperUtils.updateEntity(zooKeeper, path, entity, encoder, 1))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(IOException.class);
  }

  @Test
  public void testUpdateEntityZooKeeperError(@Mocked EntityEncoder<Object> encoder)
      throws IOException {
    Object entity = new Object();
    String path = "/somewhere";
    byte[] data = new byte[0];
    new MockUp<ZooKeeper>() {
      @Mock
      public void setData(
          final String path, byte data[], int version, StatCallback cb, Object ctx) {
        cb.processResult(Code.BADVERSION.intValue(), path, ctx, null);
      }
    };
    new Expectations() {
      {
        encoder.encode(entity);
        result = data;
      }
    };
    assertThat(ZooKeeperUtils.updateEntity(zooKeeper, path, entity, encoder, 1))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testSaveEntity(@Mocked EntityEncoder<Object> encoder) throws IOException {
    Object entity = new Object();
    String path = "/somewhere";
    byte[] data = new byte[0];
    new MockUp<ZooKeeper>() {
      @Mock
      public void create(
          final String path,
          byte data[],
          List<ACL> acl,
          CreateMode createMode,
          StringCallback cb,
          Object ctx) {
        cb.processResult(Code.OK.intValue(), path, ctx, path);
      }
    };
    new Expectations() {
      {
        encoder.encode(entity);
        result = data;
      }
    };
    assertThat(ZooKeeperUtils.saveEntity(zooKeeper, path, entity, encoder))
        .isDone()
        .isCompletedWithValue(entity);
  }

  @Test
  public void testSaveEntityEncodeError(@Mocked EntityEncoder<Object> encoder) throws IOException {
    Object entity = new Object();
    String path = "/somewhere";
    byte[] data = new byte[0];
    new Expectations() {
      {
        encoder.encode(entity);
        result = new IOException();
      }
    };
    assertThat(ZooKeeperUtils.saveEntity(zooKeeper, path, entity, encoder))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(IOException.class);
  }

  @Test
  public void testSaveEntityZooKeeperError(@Mocked EntityEncoder<Object> encoder)
      throws IOException {
    Object entity = new Object();
    String path = "/somewhere";
    byte[] data = new byte[0];
    new MockUp<ZooKeeper>() {
      @Mock
      public void create(
          final String path,
          byte data[],
          List<ACL> acl,
          CreateMode createMode,
          StringCallback cb,
          Object ctx) {
        cb.processResult(Code.NODEEXISTS.intValue(), path, ctx, path);
      }
    };
    new Expectations() {
      {
        encoder.encode(entity);
        result = data;
      }
    };
    assertThat(ZooKeeperUtils.saveEntity(zooKeeper, path, entity, encoder))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testTransaction(
      @Mocked Function<Transaction, CompletableFuture<Object>> function,
      @Mocked Transaction transaction) {
    Object entity = new Object();
    new MockUp<Transaction>() {
      @Mock
      public void commit(MultiCallback cb, Object ctx) {
        cb.processResult(Code.OK.intValue(), null, ctx, Collections.emptyList());
      }
    };
    new Expectations() {
      {
        zooKeeper.transaction();
        result = transaction;
        function.apply((Transaction) any);
        result = CompletableFuture.completedFuture(entity);
      }
    };
    assertThat(ZooKeeperUtils.transactionalOperation(zooKeeper, function))
        .isDone()
        .isCompletedWithValue(entity);
  }

  @Test
  public void testTransactionZooKeeperError(
      @Mocked Transaction transaction) {
    Object entity = new Object();
    new MockUp<Transaction>() {
      @Mock
      public void commit(MultiCallback cb, Object ctx) {
        cb.processResult(Code.NOAUTH.intValue(), null, ctx, Collections.emptyList());
      }
    };
    Function<Transaction, CompletableFuture<Object>> function = (tran) -> {
      tran.check("", 1);
      return CompletableFuture.completedFuture(entity);
    };
    new Expectations() {
      {
        zooKeeper.transaction();
        result = transaction;
      }
    };
    assertThat(ZooKeeperUtils.transactionalOperation(zooKeeper, function))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(KeeperException.class);
  }

  @Test
  public void testTransactionFunctionError(
      @Mocked Function<Transaction, CompletableFuture<Object>> function,
      @Mocked Transaction transaction) {
    Object entity = new Object();
    new Expectations() {
      {
        zooKeeper.transaction();
        result = transaction;
        function.apply((Transaction) any);
        CompletableFuture<Object> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException());
        result = future;
      }
    };
    assertThat(ZooKeeperUtils.transactionalOperation(zooKeeper, function))
        .isDone()
        .isCompletedExceptionally()
        .hasFailedWithThrowableThat()
        .isInstanceOf(RuntimeException.class);
  }
}
