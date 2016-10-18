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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;

/**
 * This is a wrap class of a ReadLock.
 */
public class InstrumentedReadLock extends InstrumentedLock {

  private ReentrantReadWriteLock readWriteLock;

  /**
   * Uses the ThreadLocal to keep the time of acquiring locks since
   * there can be multiple threads that hold the read lock concurrently.
   */
  private transient ThreadLocal<Long> readLockHeldTimeStamp =
      new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return Long.MAX_VALUE;
    };
  };

  public InstrumentedReadLock(String name, Log logger,
      ReentrantReadWriteLock readWriteLock,
      long minLoggingGapMs, long lockWarningThresholdMs) {
    this(name, logger, readWriteLock, minLoggingGapMs, lockWarningThresholdMs,
        new Timer());
  }

  public InstrumentedReadLock(String name, Log logger,
      ReentrantReadWriteLock readWriteLock,
      long minLoggingGapMs, long lockWarningThresholdMs, Timer clock) {
    super(name, logger, readWriteLock.readLock(), minLoggingGapMs,
        lockWarningThresholdMs, clock);
    this.readWriteLock = readWriteLock;
  }

  @Override
  public void lock() {
    lock.lock();
    recordLockAcquireTimestamp(clock.monotonicNow());
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    lock.lockInterruptibly();
    recordLockAcquireTimestamp(clock.monotonicNow());
  }

  @Override
  public boolean tryLock() {
    if (lock.tryLock()) {
      recordLockAcquireTimestamp(clock.monotonicNow());
      return true;
    }
    return false;
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    if (lock.tryLock(time, unit)) {
      recordLockAcquireTimestamp(clock.monotonicNow());
      return true;
    }
    return false;
  }

  @Override
  public void unlock() {
    boolean needReport = readWriteLock.getReadHoldCount() == 1;
    long localLockReleaseTime = clock.monotonicNow();
    long localLockAcquireTime = readLockHeldTimeStamp.get();
    lock.unlock();
    if (needReport) {
      readLockHeldTimeStamp.remove();
      check(localLockAcquireTime, localLockReleaseTime);
    }
  }

  /**
   * Records the time of acquiring the read lock to ThreadLocal.
   * @param lockAcquireTimestamp the time of acquiring the read lock.
   */
  private void recordLockAcquireTimestamp(long lockAcquireTimestamp) {
    if (readWriteLock.getReadHoldCount() == 1) {
      readLockHeldTimeStamp.set(lockAcquireTimestamp);
    }
  }
}
