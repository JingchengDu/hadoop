/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is a wrap class of a ReentrantReadWriteLock.
 */
public class ReadWriteLockWrapper {

  private final ReentrantReadWriteLock lock;
  private final AutoCloseableReadLock readLock;
  private final AutoCloseableWriteLock writeLock;

  public ReadWriteLockWrapper(boolean fair) {
    lock = new ReentrantReadWriteLock(fair);
    readLock = new AutoCloseableReadLock();
    writeLock = new AutoCloseableWriteLock();
  }

  public AutoCloseableReadLock getAutoCloseableReadLock() {
    return readLock;
  }

  public AutoCloseableWriteLock getAutoCloseableWriteLock() {
    return writeLock;
  }

  public class AutoCloseableReadLock extends AutoCloseableLock {
    private final ReentrantReadWriteLock.ReadLock readLock;

    public AutoCloseableReadLock() {
      readLock = lock.readLock();
    }

    @Override
    public AutoCloseableLock acquire() {
      readLock.lock();
      return this;
    }

    @Override
    public void release() {
      readLock.unlock();
    }

    @Override
    public void close() {
      release();
    }

    @Override
    public boolean tryLock() {
      return readLock.tryLock();
    }

    @Override
    public boolean isLocked() {
      return lock.isWriteLocked();
    }
  }

  public class AutoCloseableWriteLock extends AutoCloseableLock {
    private final ReentrantReadWriteLock.WriteLock writeLock;

    public AutoCloseableWriteLock() {
      writeLock = lock.writeLock();
    }

    @Override
    public AutoCloseableLock acquire() {
      writeLock.lock();
      return this;
    }

    @Override
    public void release() {
      writeLock.unlock();
    }

    @Override
    public void close() {
      release();
    }

    @Override
    public boolean tryLock() {
      return writeLock.tryLock();
    }

    @Override
    public boolean isLocked() {
      return lock.isWriteLocked();
    }
  }
}
