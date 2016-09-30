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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This is a wrap class of a instrumented ReentrantReadWriteLock.
 */
@InterfaceAudience.Private
public class InstrumentedAutoCloseableReadWriteLockWrapper {

  private final ReentrantReadWriteLock lock;
  private final InstrumentedAutoCloseableReadLock readLock;
  private final InstrumentedAutoCloseableWriteLock writeLock;

  public InstrumentedAutoCloseableReadWriteLockWrapper(boolean fair,
      String name, Log logger, long minLoggingGapMs,
      long lockWarningThresholdMs) {
    lock = new ReentrantReadWriteLock(fair);
    readLock = new InstrumentedAutoCloseableReadLock(new InstrumentedReadLock(
        lock, name, logger, minLoggingGapMs, lockWarningThresholdMs));
    writeLock = new InstrumentedAutoCloseableWriteLock(
        new InstrumentedWriteLock(lock, name, logger, minLoggingGapMs,
        lockWarningThresholdMs));
  }

  /**
   * Returns the lock used for reading.
   */
  public InstrumentedAutoCloseableReadLock readLock() {
    return readLock;
  }

  /**
   * Returns the lock used for writing.
   */
  public InstrumentedAutoCloseableWriteLock writeLock() {
    return writeLock;
  }

  /**
   * This is a wrap class of a InstrumentedReadLock.
   * Extending AutoCloseableLock so that users can use a
   * try-with-resource syntax.
   */
  public class InstrumentedAutoCloseableReadLock extends AutoCloseableLock {
    private final InstrumentedReadLock readLock;

    public InstrumentedAutoCloseableReadLock(InstrumentedReadLock readLock) {
      this.readLock = readLock;
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

    @Override
    public Condition newCondition() {
      return readLock.newCondition();
    }
  }

  /**
   * This is a wrap class of a InstrumentedWriteLock.
   * Extending AutoCloseableLock so that users can use a
   * try-with-resource syntax.
   */
  public class InstrumentedAutoCloseableWriteLock extends AutoCloseableLock {
    private final InstrumentedWriteLock writeLock;

    public InstrumentedAutoCloseableWriteLock(InstrumentedWriteLock writeLock) {
      this.writeLock = writeLock;
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

    @Override
    public Condition newCondition() {
      return writeLock.newCondition();
    }
  }
}
