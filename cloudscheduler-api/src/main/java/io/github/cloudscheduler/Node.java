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

import io.github.cloudscheduler.util.UuidUtils;
import java.io.Serializable;
import java.util.UUID;

/**
 * A scheduler node object. All scheduler workers hold a scheduler node object and register itself
 * to master with this node object.
 *
 * <p>This object implemented hashCode and equals, it's safe to use it as key in HashMap
 *
 * @author Wei Gao
 */
public class Node implements Serializable {
  private final UUID id;

  public Node() {
    this(UUID.randomUUID());
  }

  public Node(UUID id) {
    this.id = id;
  }

  /**
   * Constructor.
   *
   * @param bytes UUID bytes
   */
  public Node(byte[] bytes) {
    if (bytes == null) {
      throw new NullPointerException("bytes cannot be null");
    }
    if (bytes.length != 16) {
      throw new IllegalArgumentException("Invalid byte array.");
    }
    this.id = UuidUtils.asUuid(bytes);
  }

  public UUID getId() {
    return id;
  }

  /**
   * Get id as byte array.
   *
   * @return byte array of id
   */
  public byte[] getAsBytes() {
    return UuidUtils.asBytes(id);
  }

  @Override
  public int hashCode() {
    return 13 + id.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Node)) {
      return false;
    }
    final Node obj = (Node) other;
    return id.equals(obj.id);
  }

  @Override
  public String toString() {
    return "SchedulerNode[" + id + "]";
  }
}
