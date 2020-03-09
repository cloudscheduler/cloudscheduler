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
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A completable future that holding zookeeper.
 *
 * @author Wei Gao
 */
class ZooKeeperConnector extends CompletableFuture<ZooKeeper> {
  private static final Logger logger = LoggerFactory.getLogger(ZooKeeperConnector.class);
  private final AtomicBoolean connected;
  private ZooKeeper zooKeeper;

  /**
   * Constructor.
   *
   * @param zkUrl     zookeeper url
   * @param zkTimeout zookeeper timeout value
   * @param listener  zookeeper disconnect event listener
   */
  ZooKeeperConnector(String zkUrl, int zkTimeout, Consumer<EventType> listener) {
    Objects.requireNonNull(zkUrl, "ZooKeeper url is mandatory");
    connected = new AtomicBoolean(false);

    try {
      zooKeeper = new ZooKeeper(zkUrl, zkTimeout, event -> {
        logger.debug("Connect to zookeeper get watched event: {}", event);
        switch (event.getState()) {
          case SyncConnected:
          case ConnectedReadOnly:
            connected.set(true);
            this.complete(zooKeeper);
            break;
          case Disconnected:
          case Expired:
            connected.set(false);
            if (listener != null) {
              listener.accept(EventType.CONNECTION_LOST);
            }
            break;
          default:
            this.completeExceptionally(new KeeperException.ConnectionLossException());
        }
      });
    } catch (IOException e) {
      this.completeExceptionally(e);
    }
  }

  /**
   * Cancel connecting to zookeeper.
   *
   * @param mayInterruptIfRunning allow interrupt
   * @return {@code true} if canceled
   */
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (!connected.get() && zooKeeper != null) {
      try {
        zooKeeper.close();
        logger.trace("Zookeeper client closed.");
      } catch (InterruptedException e) {
        logger.trace("Close zookeeper got interrupted", e);
      }
    }
    return super.cancel(mayInterruptIfRunning);
  }
}
