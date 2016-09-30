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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;

/**
 * This is a wrap class of a instrumented ReentrantReadWriteLock.
 */
@InterfaceAudience.Private
public class InstrumentedAutoCloseableReadWriteLockWrapper {

  private final ReentrantReadWriteLock lock;
  private final InstrumentedAutoCloseableReadLock readLock;
  private final InstrumentedAutoCloseableWriteLock writeLock;
  /** Minimum gap between two lock warnings. */
  private final long minLoggingGapMs;
  /** Threshold for detecting long lock held time. */
  private final long lockWarningThresholdMs;
  private final Log logger;
  private final String name;

  public InstrumentedAutoCloseableReadWriteLockWrapper(boolean fair,
      String name, Log logger, long minLoggingGapMs,
      long lockWarningThresholdMs) {
    this(new ReentrantReadWriteLock(fair), name, logger, minLoggingGapMs,
        lockWarningThresholdMs);
  }

  @VisibleForTesting
  public InstrumentedAutoCloseableReadWriteLockWrapper(
      ReentrantReadWriteLock lock, String name, Log logger,
      long minLoggingGapMs,
      long lockWarningThresholdMs) {
    this.minLoggingGapMs = minLoggingGapMs;
    this.lockWarningThresholdMs = lockWarningThresholdMs;
    this.logger = logger;
    this.name = name;
    this.lock = lock;
    readLock = new InstrumentedAutoCloseableReadLock(lock);
    writeLock = new InstrumentedAutoCloseableWriteLock(lock);
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

  @VisibleForTesting
  ReentrantReadWriteLock getReentrantReadWriteLock() {
    return lock;
  }

  /**
   * This is a wrap class of a ReadLock.
   * Extending AutoCloseableLock so that users can use a
   * try-with-resource syntax.
   */
  public class InstrumentedAutoCloseableReadLock extends AutoCloseableLock {
    private final Timer clock;
    // Tracking counters for the read lock statistics.
    private final AtomicLong lastLogTimestamp;
    private final AtomicLong warningsSuppressed = new AtomicLong(0);
    private final ReadLock readLock;

    /**
     * Uses the ThreadLocal to keep the time of acquiring locks since
     * there can be multiple threads that hold the read lock concurrently.
     */
    private ThreadLocal<Long> readLockHeldTimeStamp =
        new ThreadLocal<Long>() {
      @Override
      protected Long initialValue() {
        return Long.MAX_VALUE;
      };
    };

    public InstrumentedAutoCloseableReadLock(
        ReentrantReadWriteLock readWriteLock) {
      this(readWriteLock, new Timer());
    }

    @VisibleForTesting
    InstrumentedAutoCloseableReadLock(
        ReentrantReadWriteLock readWriteLock, Timer clock) {
      this.readLock = readWriteLock.readLock();
      this.clock = clock;
      lastLogTimestamp = new AtomicLong(clock.monotonicNow()
          - Math.max(minLoggingGapMs, lockWarningThresholdMs));
    }

    @Override
    public AutoCloseableLock acquire() {
      readLock.lock();
      recordLockAcquireTimestamp(clock.monotonicNow());
      return this;
    }

    @Override
    public void release() {
      boolean needReport = lock.getReadHoldCount() == 1;
      long lockHeldTime = clock.monotonicNow() - readLockHeldTimeStamp.get();
      readLock.unlock();
      if (needReport) {
        readLockHeldTimeStamp.remove();
        check(clock, lockHeldTime, lastLogTimestamp, warningsSuppressed, true);
      }
    }

    @Override
    public void close() {
      release();
    }

    @Override
    public boolean tryLock() {
      if (readLock.tryLock()) {
        recordLockAcquireTimestamp(clock.monotonicNow());
        return true;
      }
      return false;
    }

    @Override
    public boolean isLocked() {
      return lock.isWriteLocked();
    }

    @Override
    public Condition newCondition() {
      return readLock.newCondition();
    }

    /**
     * Records the time of acquiring the read lock to ThreadLocal.
     * @param lockAcquireTimestamp the time of acquiring the read lock.
     */
    private void recordLockAcquireTimestamp(long lockAcquireTimestamp) {
      if (lock.getReadHoldCount() == 1) {
        readLockHeldTimeStamp.set(lockAcquireTimestamp);
      }
    }
  }

  /**
   * This is a wrap class of a WriteLock.
   * Extending AutoCloseableLock so that users can use a
   * try-with-resource syntax.
   */
  public class InstrumentedAutoCloseableWriteLock extends AutoCloseableLock {
    private final transient Timer clock;
    // Tracking counters for the read lock statistics.
    private volatile long lockAcquireTimestamp;
    private final AtomicLong lastLogTimestamp;
    private final AtomicLong warningsSuppressed = new AtomicLong(0);
    private final WriteLock writeLock;

    public InstrumentedAutoCloseableWriteLock(
        ReentrantReadWriteLock readWriteLock) {
      this(readWriteLock, new Timer());
    }

    @VisibleForTesting
    InstrumentedAutoCloseableWriteLock(ReentrantReadWriteLock readWriteLock,
        Timer clock) {
      this.writeLock = readWriteLock.writeLock();
      this.clock = clock;
      lastLogTimestamp = new AtomicLong(clock.monotonicNow()
          - Math.max(minLoggingGapMs, lockWarningThresholdMs));
    }

    @Override
    public AutoCloseableLock acquire() {
      writeLock.lock();
      lockAcquireTimestamp = clock.monotonicNow();
      return this;
    }

    @Override
    public void release() {
      long localLockReleaseTime = clock.monotonicNow();
      long localLockAcquireTime = lockAcquireTimestamp;
      writeLock.unlock();
      check(clock, localLockReleaseTime - localLockAcquireTime,
          lastLogTimestamp, warningsSuppressed, false);
    }

    @Override
    public void close() {
      release();
    }

    @Override
    public boolean tryLock() {
      if (writeLock.tryLock()) {
        lockAcquireTimestamp = clock.monotonicNow();
        return true;
      }
      return false;
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

  /**
   * Logs a warning if the lock was held for too long.
   *
   * Should be invoked by the caller immediately AFTER releasing the lock.
   *
   */
  private void check(Timer clock, long lockHeldTime,
      AtomicLong lastLogTimestamp,
      AtomicLong warningsSuppressed, boolean readLock) {
    if (!logger.isWarnEnabled()) {
      return;
    }

    if (lockWarningThresholdMs - lockHeldTime < 0) {
      long now;
      long localLastLogTs;
      do {
        now = clock.monotonicNow();
        localLastLogTs = lastLogTimestamp.get();
        long deltaSinceLastLog = now - localLastLogTs;
        // check should print log or not
        if (deltaSinceLastLog - minLoggingGapMs < 0) {
          warningsSuppressed.incrementAndGet();
          return;
        }
      } while (!lastLogTimestamp.compareAndSet(localLastLogTs, now));
      long suppressed = warningsSuppressed.getAndSet(0);
      logWarning(lockHeldTime, suppressed, readLock);
    }
  }

  @VisibleForTesting
  void logWarning(long lockHeldTime, long suppressed, boolean readLock) {
    logger.warn(String.format("%s lock held time above threshold: " +
        "lock identifier: %s " +
        "lockHeldTimeMs=%d ms. Suppressed %d lock warnings. " +
        "The stack trace is: %s" ,
        readLock ? "Read" : "Write", name,
        lockHeldTime, suppressed,
        StringUtils.getStackTrace(Thread.currentThread())));
  }
}
