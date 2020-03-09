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

import io.github.cloudscheduler.EventType;
import io.github.cloudscheduler.codec.EntityDecoder;
import io.github.cloudscheduler.codec.EntityEncoder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class that provide zookeeper related operations, convert zookeeper async operation
 * into CompletableFuture.
 *
 * @author Wei Gao
 */
public class ZooKeeperUtils {
  public static final List<ACL> DEFAULT_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  private static final Logger logger = LoggerFactory.getLogger(ZooKeeperUtils.class);

  /**
   * Open a ZooKeeper async. Return a CompletableFuture&lt;ZooKeeper&gt;, so it can be chained.
   *
   * @param zooKeeperUrl the zookeeper url
   * @param zkTimeout    timeout value
   * @return CompletableFuture&lt;ZooKeeper&gt;
   */
  public static CompletableFuture<ZooKeeper> connectToZooKeeper(String zooKeeperUrl,
                                                                int zkTimeout) {
    return connectToZooKeeper(zooKeeperUrl, zkTimeout, null);
  }

  public static CompletableFuture<ZooKeeper> connectToZooKeeper(
      String zooKeeperUrl,
      int zkTimeout,
      Consumer<EventType> disconnectListener) {
    Objects.requireNonNull(zooKeeperUrl, "ZooKeeper url is mandatory");
    return new ZooKeeperConnector(zooKeeperUrl, zkTimeout, disconnectListener);
  }

  /**
   * Create znodes recursively.
   * If znodes on the path already exist, do nothing, otherwise create znode without data.
   * It will return a CompletableFuture&lt;String&gt; so it can be chained.
   *
   * @param zooKeeper ZooKeeper object
   * @param path      Path of znodes
   * @return CompletableFuture that hold path of created znode.
   */
  public static CompletableFuture<String> createZnodes(ZooKeeper zooKeeper,
                                                       String path) {
    Objects.requireNonNull(zooKeeper, "ZooKeeper is mandatory");
    Objects.requireNonNull(path, "Path is mandatory");
    logger.trace("Creating path {} on zookeeper: {}", path, zooKeeper);
    String[] ps = path.split("/");
    CompletableFuture<String> future = CompletableFuture.completedFuture("");
    try {
      for (String p : ps) {
        if (!p.isEmpty()) {
          logger.trace("Check/Create znode: {}", p);
          future = future.thenCompose(pa -> putData(zooKeeper,
              pa + "/" + p));
        }
      }
    } catch (Throwable e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Create znode. This API will create znode with more customizable settings, but will fail
   * if parent znode not exists. If znode already exist, it will update data of the znode.
   * If will return a CompletableFuture&lt;String&gt; so it can be chained.
   *
   * @param zooKeeper ZooKeeper object
   * @param path      Full path of znode
   * @param mode      Create mode
   * @param data      Data that going to put
   * @return CompletableFuture that hold path of created znode
   */
  public static CompletableFuture<String> createZnode(ZooKeeper zooKeeper,
                                                      String path,
                                                      CreateMode mode,
                                                      byte[] data) {
    return putData(zooKeeper, path, data, mode, false);
  }

  /**
   * Get list of children on path.
   *
   * @param zooKeeper ZooKeeper object
   * @param dest      The path
   * @return CompletableFuture that hold list of children
   */
  public static CompletableFuture<List<String>> getChildren(ZooKeeper zooKeeper,
                                                            String dest) {
    return getChildren(zooKeeper, dest, null);
  }

  /**
   * Get children of giving znode, set callback when children changed.
   *
   * @param zooKeeper ZooKeeper Client
   * @param dest      Path to znode to get children
   * @param listener  Callback when children changed
   * @return CompletableFuture of list of children
   */
  public static CompletableFuture<List<String>> getChildren(ZooKeeper zooKeeper,
                                                            String dest,
                                                            Consumer<EventType> listener) {
    Objects.requireNonNull(zooKeeper, "ZooKeeper is mandatory");
    Objects.requireNonNull(dest, "Path is mandatory");
    CompletableFuture<List<String>> future = new CompletableFuture<>();
    try {
      zooKeeper.getChildren(dest,
          listener == null ? null : event -> {
            logger.trace("Got event: {}", event);
            switch (event.getType()) {
              case NodeChildrenChanged:
                listener.accept(EventType.CHILD_CHANGED);
                break;
              case NodeDeleted:
                listener.accept(EventType.ENTITY_UPDATED);
                break;
              default:
                break;
            }
          },
          (rc, path, ctx, children, stat) -> {
            try {
              logger.trace("Get children on node {} done, return code: {}", path, rc);
              KeeperException.Code code = KeeperException.Code.get(rc);
              if (code.equals(KeeperException.Code.OK)) {
                future.complete(children);
              } else {
                logger.debug("Result code is not OK, throw exception");
                future.completeExceptionally(KeeperException.create(code, path));
              }
            } catch (Throwable e) {
              future.completeExceptionally(e);
            }
          }, null);
    } catch (Throwable e) {
      future.completeExceptionally(e);
    }
    return future;
  }


  /**
   * Wait till a znode gone.
   *
   * @param zooKeeper ZooKeeper Client
   * @param dest      znode path
   * @return CompletableFuture
   */
  public static CompletableFuture<Void> waitTillGone(ZooKeeper zooKeeper,
                                                     String dest) {
    Objects.requireNonNull(zooKeeper, "ZooKeeper is mandatory");
    Objects.requireNonNull(dest, "Path is mandatory");
    logger.trace("Waiting for {} gone", dest);
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      zooKeeper.exists(dest,
          event -> {
            logger.trace("Got event: {}", event);
            if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
              future.complete(null);
            }
          }, (rc, path, ctx, stat) -> {
            try {
              KeeperException.Code code = KeeperException.Code.get(rc);
              switch (code) {
                case OK:
                  logger.trace("Find node {}.", path);
                  break;
                case NONODE:
                  logger.trace("Didn't find node {}.", path);
                  future.complete(null);
                  break;
                default:
                  logger.debug("Do not recognize result code, throw exception");
                  future.completeExceptionally(KeeperException.create(code, path));
                  break;
              }
            } catch (Throwable e) {
              future.completeExceptionally(e);
            }
          }, null);
    } catch (Throwable e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Delete a znode if exist.
   *
   * @param zooKeeper ZooKeeper Client
   * @param dest      znode path to be delete
   * @return CompletableFuture that hold delete znode path
   */
  public static CompletableFuture<Void> deleteIfExists(ZooKeeper zooKeeper,
                                                       String dest) {
    Objects.requireNonNull(zooKeeper, "ZooKeeper is mandatory");
    Objects.requireNonNull(dest, "Path is mandatory");
    logger.trace("Deleting node at {}", dest);
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      zooKeeper.exists(dest, false, (rc, path, ctx, stat) -> {
        try {
          KeeperException.Code code = KeeperException.Code.get(rc);
          switch (code) {
            case OK:
              logger.trace("Find node {}.", dest);
              int version = stat.getVersion();
              zooKeeper.delete(path, version, (rc1, path1, ctx1) -> {
                logger.trace("Delete {} done.", dest);
                KeeperException.Code code1 = KeeperException.Code.get(rc1);
                switch (code1) {
                  case OK:
                    logger.trace("Node: {} deleted.", path1);
                    future.complete(null);
                    break;
                  default:
                    future.completeExceptionally(KeeperException.create(code1, path1));
                    break;
                }
              }, null);
              break;
            case NONODE:
              logger.trace("Didn't find node {}.", path);
              future.complete(null);
              break;
            default:
              logger.debug("Do not recognize result code, throw exception");
              future.completeExceptionally(KeeperException.create(code, path));
              break;
          }
        } catch (Throwable e) {
          future.completeExceptionally(e);
        }
      }, null);
    } catch (Throwable e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Delete a znode.
   *
   * @param zooKeeper ZooKeeper Client
   * @param dest      Path to the znode to be deleted
   * @param recursive if delete children
   * @return CompletableFuture
   */
  public static CompletableFuture<Void> deleteIfExists(ZooKeeper zooKeeper,
                                                       String dest,
                                                       boolean recursive) {
    logger.trace("Deleting {}, recursive: {}", dest, recursive);
    if (recursive) {
      List<CompletableFuture<Void>> fs = new ArrayList<>();
      return getChildren(zooKeeper, dest).thenCompose(children -> {
        if (children != null && !children.isEmpty()) {
          children.forEach(child -> {
            logger.trace("Delete child {} under {}", child, dest);
            fs.add(deleteIfExists(zooKeeper, dest + "/" + child, recursive));
          });
        }
        if (!fs.isEmpty()) {
          return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]));
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }).thenCompose(v -> deleteIfExists(zooKeeper, dest));
    } else {
      return deleteIfExists(zooKeeper, dest);
    }
  }

  private static CompletableFuture<String> putData(ZooKeeper zooKeeper,
                                                   String dest) {
    CompletableFuture<String> future = new CompletableFuture<>();
    try {
      logger.trace("Creating empty znode at {} on zookeeper: {}", dest, zooKeeper);
      zooKeeper.exists(dest, false, (rc, path, ctx, stat) -> {
        try {
          logger.trace("Check if {} exists return code: {}", path, rc);
          KeeperException.Code code = KeeperException.Code.get(rc);
          switch (code) {
            case OK:
              logger.trace("Found node on {}.", path);
              future.complete(path);
              break;
            case NONODE:
              logger.trace("Didn't find node {}, create one.", path);
              putData(zooKeeper, path, null, CreateMode.PERSISTENT, true)
                  .whenComplete((v, cause) -> {
                    if (cause != null) {
                      future.completeExceptionally(cause);
                    } else {
                      future.complete(v);
                    }
                  });
              break;
            default:
              logger.debug("Do not recognize result code, throw exception");
              future.completeExceptionally(KeeperException.create(code, path));
          }
        } catch (Throwable e) {
          future.completeExceptionally(e);
        }
      }, null);
    } catch (Throwable e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  private static CompletableFuture<String> putData(ZooKeeper zooKeeper,
                                                   String dest,
                                                   byte[] data,
                                                   CreateMode mode,
                                                   boolean successIfExist) {
    CompletableFuture<String> future = new CompletableFuture<>();
    try {
      logger.trace("Putting {} bytes to {} on zookeeper: {}", data == null ? 0
          : data.length, dest, zooKeeper);
      zooKeeper.create(dest, data, DEFAULT_ACL, mode, (rc, path, ctx, name) -> {
        try {
          logger.trace("Create node {} done, return code: {}", name, rc);
          KeeperException.Code code = KeeperException.Code.get(rc);
          switch (code) {
            case OK:
              future.complete(name);
              break;
            case NODEEXISTS:
              if (successIfExist) {
                future.complete(path);
                break;
              } else {
                logger.debug("Result code is not OK, throw exception");
                future.completeExceptionally(KeeperException.create(code, path));
                break;
              }
            default:
              logger.debug("Result code is not OK, throw exception");
              future.completeExceptionally(KeeperException.create(code, path));
              break;
          }
        } catch (Throwable e) {
          future.completeExceptionally(e);
        }
      }, null);
    } catch (Throwable e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Read data from zookeeper a-synchronized. EntityDecoder been used to convert from byte array to
   * entity type
   *
   * @param zooKeeper The ZooKeeper client
   * @param dest      Path to ZNode that need to read
   * @param decoder   Decoder that will be used to convert from byte array to entity
   * @param <T>       Entity type
   * @return CompletableFuture of entity holder. Entity holder holder entity as well as version,
   *     so it can be used to call setData later. {@code null} if znode not exist
   */
  public static <T> CompletableFuture<EntityHolder<T>> readEntity(ZooKeeper zooKeeper,
                                                                  String dest,
                                                                  EntityDecoder<T> decoder) {
    return readEntity(zooKeeper, dest, decoder, null);
  }

  /**
   * Read data from zookeeper and register a listener.
   *
   * @param zooKeeper Zookeeper client
   * @param dest      path to znode
   * @param decoder   decoder
   * @param listener  listener
   * @param <T>       entity type
   * @return CompletableFuture of entity holder
   */
  public static <T> CompletableFuture<EntityHolder<T>> readEntity(ZooKeeper zooKeeper,
                                                                  String dest,
                                                                  EntityDecoder<T> decoder,
                                                                  Consumer<EventType> listener) {
    Objects.requireNonNull(zooKeeper, "ZooKeeper is mandatory");
    Objects.requireNonNull(dest, "Path is mandatory");
    Objects.requireNonNull(decoder, "Entity decoder is mandatory");
    CompletableFuture<EntityHolder<T>> future = new CompletableFuture<>();
    Watcher watcher = listener == null ? null : (event) -> {
      switch (event.getType()) {
        case NodeDataChanged:
          listener.accept(EventType.ENTITY_UPDATED);
          break;
        case NodeDeleted:
          listener.accept(EventType.ENTITY_DELETED);
          break;
        default:
          break;
      }
    };
    try {
      zooKeeper.getData(dest, watcher, (rc, path, ctx, data, stat) -> {
        try {
          logger.trace("Get data on node: {} return code: {}", path, rc);
          KeeperException.Code code = KeeperException.Code.get(rc);
          switch (code) {
            case OK:
              logger.trace("Decode data and call supplier.");
              if (data != null) {
                try {
                  T entity = decoder.decode(data);
                  future.complete(new EntityHolder<>(entity, stat.getVersion()));
                } catch (IOException e) {
                  logger.debug("IOException when decode entity", e);
                  future.completeExceptionally(e);
                }
              } else {
                future.complete(null);
              }
              break;
            case NONODE:
              logger.trace("Didn't find znode on path: {}", dest);
              future.complete(null);
              break;
            default:
              logger.debug("Result code is not OK, throw exception");
              future.completeExceptionally(KeeperException.create(code, path));
              break;
          }
        } catch (Throwable e) {
          future.completeExceptionally(e);
        }
      }, null);
    } catch (Throwable e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Check if a znode exist. Return version as Integer. {@code null} if znode not exist
   *
   * @param zooKeeper ZooKeeper client
   * @param dest      znode path
   * @return CompletableFuture of Integer.
   */
  public static CompletableFuture<Integer> exists(ZooKeeper zooKeeper, String dest) {
    logger.trace("Checking if {} exists.", dest);
    CompletableFuture<Integer> future = new CompletableFuture<>();
    zooKeeper.exists(dest, false, (rc, path, ctx, stat) -> {
      try {
        logger.trace("Exist return code {} for path {}", rc, path);
        KeeperException.Code code = KeeperException.Code.get(rc);
        switch (code) {
          case OK:
            future.complete(stat.getVersion());
            break;
          case NONODE:
            future.complete(null);
            break;
          default:
            future.completeExceptionally(KeeperException.create(code, path));
            break;
        }
      } catch (Throwable e) {
        future.completeExceptionally(e);
      }
    }, null);
    return future;
  }

  /**
   * Create a persistent znode.
   *
   * @param zooKeeper ZooKeeper
   * @param dest      znode path
   * @param data      data
   * @return CompletableFuture of znode path
   */
  public static CompletableFuture<String> createPersistentZnode(ZooKeeper zooKeeper,
                                                                String dest,
                                                                byte[] data) {
    logger.trace("Creating persistent znode on path {}.", dest);
    return createZnode(zooKeeper, dest, CreateMode.PERSISTENT, data);
  }

  public static CompletableFuture<String> createEphemeralZnode(ZooKeeper zooKeeper,
                                                               String dest,
                                                               byte[] data) {
    logger.trace("Creating ephemeral znode on path {}.", dest);
    return createZnode(zooKeeper, dest, CreateMode.EPHEMERAL, data);
  }

  /**
   * Set data on existing znode. Use exist or readData to get version
   *
   * @param <T>       entity type
   * @param zooKeeper ZooKeeper client
   * @param dest      znode path
   * @param entity    Entity to be saved
   * @param encoder   Entity encoder
   * @param version   version
   * @return CompletableFuture of znode path
   */
  public static <T> CompletableFuture<T> updateEntity(ZooKeeper zooKeeper,
                                                      String dest,
                                                      T entity,
                                                      EntityEncoder<T> encoder,
                                                      int version) {
    CompletableFuture<T> future = new CompletableFuture<>();
    try {
      zooKeeper.setData(dest, encoder.encode(entity), version, (rc, path, ctx, stat) -> {
        try {
          logger.trace("Set node data return code {} for path {}", rc, path);
          KeeperException.Code code = KeeperException.Code.get(rc);
          switch (code) {
            case OK:
              future.complete(entity);
              break;
            default:
              future.completeExceptionally(KeeperException.create(code, path));
              break;
          }
        } catch (Throwable e) {
          future.completeExceptionally(e);
        }
      }, null);
    } catch (Throwable e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Create a znode for entity.
   *
   * @param <T>       entity type
   * @param zooKeeper ZooKeeper client
   * @param dest      znode path
   * @param entity    Entity to be saved
   * @param encoder   Entity encoder
   * @return CompletableFuture of znode path
   */
  public static <T> CompletableFuture<T> saveEntity(ZooKeeper zooKeeper,
                                                    String dest,
                                                    T entity,
                                                    EntityEncoder<T> encoder) {
    CompletableFuture<T> future = new CompletableFuture<>();
    try {
      zooKeeper.create(dest, encoder.encode(entity), DEFAULT_ACL, CreateMode.PERSISTENT,
          (rc, path, ctx, name) -> {
            try {
              logger.trace("Create node data return code {} for path {}", rc, path);
              KeeperException.Code code = KeeperException.Code.get(rc);
              switch (code) {
                case OK:
                  future.complete(entity);
                  break;
                default:
                  future.completeExceptionally(KeeperException.create(code, path));
                  break;
              }
            } catch (Throwable e) {
              future.completeExceptionally(e);
            }
          }, null);
    } catch (Throwable e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Transaction operation. It wrap the transaction logic and use callback to provide adding
   * operation to transaction object.
   *
   * @param <T>       entity type
   * @param zooKeeper ZooKeeper client
   * @param function  A function that take ZooKeeper {@code transaction} as parameter. Client use
   *                  this function to add operation to the transaction. Function returns a
   *                  CompletableFuture so it can chain with transaction operations.
   * @return CompletableFuture that hold whatever function returns
   */
  public static <T> CompletableFuture<T> transactionalOperation(
      ZooKeeper zooKeeper,
      Function<Transaction, CompletableFuture<T>> function) {
    logger.trace("Start transactional operation");
    Transaction transaction = zooKeeper.transaction();
    return function.apply(transaction).thenCompose(r -> {
      CompletableFuture<T> future = new CompletableFuture<>();
      transaction.commit((rc, path, ctx, opResults) -> {
        try {
          logger.trace("Transaction return result: {}", rc);
          KeeperException.Code code = KeeperException.Code.get(rc);
          switch (code) {
            case OK:
              future.complete(r);
              break;
            default:
              future.completeExceptionally(KeeperException.create(code, path));
              break;
          }
        } catch (Throwable e) {
          future.completeExceptionally(e);
        }
      }, null);
      return future;
    });
  }

  public static class EntityHolder<T> {
    private final T entity;
    private final int version;

    EntityHolder(T entity, int version) {
      this.entity = entity;
      this.version = version;
    }

    public T getEntity() {
      return entity;
    }

    public int getVersion() {
      return version;
    }
  }
}
