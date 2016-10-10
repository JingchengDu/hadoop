/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Test;

/**
 * A test class for AutoCloseableReadLock and AutoCloseableWriteLock.
 */
public class TestAutoCloseableReadWriteLock {

  /**
   * Tests exclusive access of the write lock.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testWriteLock() throws Exception {
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    final ThreadLocal<Boolean> locked = new ThreadLocal<Boolean>();
    locked.set(Boolean.FALSE);
    final AutoCloseableWriteLock writeLock = new AutoCloseableWriteLock(
        readWriteLock) {
      @Override
      public AutoCloseableLock acquire() {
        AutoCloseableLock lock = super.acquire();
        locked.set(Boolean.TRUE);
        return lock;
      }

      @Override
      public void release() {
        super.release();
        locked.set(Boolean.FALSE);
      }
    };
    final AutoCloseableReadLock readLock = new AutoCloseableReadLock(
        readWriteLock);
    try (AutoCloseableLock lock = writeLock.acquire()) {
      Thread competingWriteThread = new Thread() {
        @Override
        public void run() {
          assertFalse(writeLock.tryLock());
        }
      };
      competingWriteThread.start();
      competingWriteThread.join();
      Thread competingReadThread = new Thread() {
        @Override
        public void run() {
          assertFalse(readLock.tryLock());
        };
      };
      competingReadThread.start();
      competingReadThread.join();
    }
    assertFalse(locked.get());
    locked.remove();
  }

  /**
   * Tests the read lock.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testReadLock() throws Exception {
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    final AutoCloseableLock readLock = new AutoCloseableReadLock(
        readWriteLock);
    final AutoCloseableLock writeLock = new AutoCloseableWriteLock(
        readWriteLock);
    try (AutoCloseableLock lock = readLock.acquire()) {
      Thread competingReadThread = new Thread() {
        @Override
        public void run() {
          assertTrue(readLock.tryLock());
          readLock.release();
        }
      };
      competingReadThread.start();
      competingReadThread.join();
      Thread competingWriteThread = new Thread() {
        @Override
        public void run() {
          assertFalse(writeLock.tryLock());
        }
      };
      competingWriteThread.start();
      competingWriteThread.join();
    }
  }
}
