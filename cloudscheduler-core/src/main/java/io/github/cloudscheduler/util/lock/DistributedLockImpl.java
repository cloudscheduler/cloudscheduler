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

package io.github.cloudscheduler.util.lock;

import io.github.cloudscheduler.util.ZooKeeperUtils;
import io.github.cloudscheduler.util.retry.RetryStrategy;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper based distributed lock implementation.
 *
 * @author Wei Gao
 */
public class DistributedLockImpl extends CompletableFuture<Void> implements DistributedLock {
  private static final Logger logger = LoggerFactory.getLogger(DistributedLockImpl.class);

  private static final Pattern UUID_SEQUENTIAL_PATTERN =
      Pattern.compile("^(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-"
              + "[0-9a-f]{12})-(?<sequence>\\d{10})$", Pattern.CASE_INSENSITIVE);

  private final ZooKeeper zooKeeper;
  private final String lockBasePath;
  private final String lockName;
  private final UUID id;
  private final AtomicBoolean lockAcquired;
  private final RetryStrategy retryStrategy;

  private String lockPath;
  private int sequence;
  private volatile CompletableFuture<Void> lockingFuture;

  /**
   * Constructor.
   *
   * @param id unique id
   * @param zooKeeper zooKeeper
   * @param lockBasePath lock base path
   * @param lockName lock name
   */
  public DistributedLockImpl(UUID id, ZooKeeper zooKeeper, String lockBasePath, String lockName) {
    Objects.requireNonNull(id, "ID is mandatory");
    Objects.requireNonNull(zooKeeper, "ZooKeeper is mandatory");
    Objects.requireNonNull(lockBasePath, "Lock base path is mandatory");
    logger.debug("{}: Creating distributed lock {} on {} under folder: {}",
        id, lockName, zooKeeper, lockBasePath);
    this.id = id;
    this.zooKeeper = zooKeeper;
    this.lockBasePath = (lockBasePath.startsWith("/") ? "" : "/")
        + lockBasePath + (lockName == null ? "" : ("/" + lockName));
    if (lockName == null) {
      this.lockName = lockBasePath.substring(lockBasePath.lastIndexOf('/') + 1);
    } else {
      this.lockName = lockName;
    }
    this.lockAcquired = new AtomicBoolean(false);
    //noinspection
    this.retryStrategy = RetryStrategy.newBuilder()
        .fibonacci(250L)
        .random()
        .maxRetry(10)
        .retryOn(Collections.singletonList(KeeperException.class))
        .stopAt(Arrays.asList(KeeperException.NoAuthException.class,
            KeeperException.SessionExpiredException.class,
            KeeperException.ConnectionLossException.class))
        .build();
    ZooKeeperUtils.createZnodes(zooKeeper, this.lockBasePath)
        .thenAccept(path -> this.complete(null));
  }

  @Override
  public CompletableFuture<Void> lock() {
    logger.debug("{}-{}: Trying to acquire lock.", id, lockName);
    if (lockingFuture == null) {
      synchronized (this) {
        if (lockingFuture == null) {
          lockingFuture = thenCompose(v -> retryStrategy.call(() ->
              createOrGetExistingEphemeralNode(lockBasePath)
                  .thenCompose(children -> processLock(lockBasePath, children))));
        }
      }
    }
    return lockingFuture;
  }

  @Override
  public CompletableFuture<Void> unlock() {
    logger.trace("Unlock");
    return thenCompose(v -> retryStrategy.call(() -> {
      if (lockAcquired.compareAndSet(true, false)) {
        logger.trace("Lock acquired set to false");
      }
      CompletableFuture<Void> f = lockingFuture;
      lockingFuture = null;
      if (f != null) {
        f.cancel(true);
      }
      if (lockPath != null) {
        String nodeName = lockBasePath + "/" + lockPath;
        return ZooKeeperUtils.deleteIfExists(zooKeeper, nodeName);
      }
      return CompletableFuture.completedFuture(null);
    }));
  }

  private SortedMap<Integer, String> getOrderedChildren(List<String> children) {
    SortedMap<Integer, String> result = new TreeMap<>();
    children.forEach(p -> {
      String nodeName = p;
      int i = p.lastIndexOf('/');
      if (i >= 0) {
        nodeName = p.substring(i);
      }
      Matcher m = UUID_SEQUENTIAL_PATTERN.matcher(nodeName);
      if (m.matches()) {
        int seq = Integer.parseInt(m.group("sequence"));
        result.put(seq, p);
      }
    });
    return result;
  }

  private Map<Integer, String> getExistingNode(List<String> children) {
    for (String p : children) {
      Map<Integer, UUID> map = parseSequence(p);
      if (map != null) {
        Map.Entry<Integer, UUID> entry = map.entrySet().iterator().next();
        if (id.equals(entry.getValue())) {
          Map<Integer, String> result = new HashMap<>(1);
          result.put(entry.getKey(), p);
          return result;
        }
      }
    }
    return null;
  }

  private Map<Integer, UUID> parseSequence(String path) {
    Matcher m = UUID_SEQUENTIAL_PATTERN.matcher(path);
    if (m.matches()) {
      UUID uuid = UUID.fromString(m.group("uuid"));
      int seq = Integer.parseInt(m.group("sequence"));
      Map<Integer, UUID> result = new HashMap<>(1);
      result.put(seq, uuid);
      return result;
    }
    return null;
  }

  private CompletableFuture<List<String>> createOrGetExistingEphemeralNode(String base) {
    return retryStrategy.call(() -> ZooKeeperUtils.getChildren(zooKeeper, base)
        .thenCompose(children -> {
          logger.trace("List children under {} return: {}", base, children);
          Map<Integer, String> existing = getExistingNode(children);
          if (existing != null) {
            Map.Entry<Integer, String> e = existing.entrySet().iterator().next();
            this.sequence = e.getKey();
            this.lockPath = e.getValue();
            logger.trace("{}-{}: Found existing znode for this lock, sequence: {}",
                id, lockName, sequence);
            return CompletableFuture.completedFuture(children);
          } else {
            return ZooKeeperUtils.createZnode(zooKeeper,
                base + "/" + id.toString() + "-",
                CreateMode.EPHEMERAL_SEQUENTIAL, null)
                .thenCompose(path -> {
                  logger.trace("Create ephemeral sequential node return path: {}", path);
                  return createOrGetExistingEphemeralNode(base);
                });
          }
        }));
  }

  private CompletableFuture<Void> processLock(String basePath, List<String> children) {
    logger.trace("Processing lock, base path: {}, children: {}", basePath, children);
    SortedMap<Integer, String> kids = getOrderedChildren(children);
    int smallest = -1;
    for (Map.Entry<Integer, String> entry : kids.entrySet()) {
      if (this.sequence == entry.getKey()) {
        if (smallest < 0) {
          logger.trace("Lock acquired");
          lockAcquired.set(true);
          return CompletableFuture.completedFuture(null);
        } else {
          String nextPath = kids.get(smallest);
          return thenCompose(n -> retryStrategy.call(() -> ZooKeeperUtils
              .waitTillGone(zooKeeper, basePath + "/" + nextPath)
              .thenCompose(v -> {
                logger.trace("{} gone.", nextPath);
                return ZooKeeperUtils.getChildren(zooKeeper, basePath)
                    .thenCompose(cs -> processLock(basePath, cs));
              })));
        }
      } else if (this.sequence > entry.getKey()) {
        smallest = entry.getKey();
      } else {
        break;
      }
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new IllegalStateException("This is impossible"));
    return future;
  }
}
