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

package io.github.cloudscheduler.model;

/**
 * JobInstance state enum.
 *
 * @author Wei Gao
 */
public enum JobInstanceState {
  SCHEDULED,    // Scheduled, but not start yet.
  RUNNING,      // Currently running
  NODE_FAILED,  // Running node failed
  COMPLETE,     // Complete successfully
  FAILED;       // Complete but failed

  /**
   * Check if JobInstance state is complete. For global job, NODE_FAILED consider
   * as complete. Otherwise, NODE_FAILED consider as not complete.
   *
   * @param global global flag
   * @return {@code true} if state is complete, {@code false} otherwise.
   */
  public boolean isComplete(boolean global) {
    switch (this) {
      case COMPLETE:
      case FAILED:
        return true;
      case NODE_FAILED:
        if (global) {
          return true;
        } else {
          return false;
        }
      default:
        return false;
    }
  }
}
