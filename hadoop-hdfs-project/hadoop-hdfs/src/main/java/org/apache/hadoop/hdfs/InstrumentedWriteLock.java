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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Timer;

/**
 * This is a wrap class of a WriteLock.
 */
public class InstrumentedWriteLock extends WriteLock {

  private static final long serialVersionUID = 1L;

  private WriteLock writeLock;
  private final String name;
  private transient Log logger =
      LogFactory.getLog(InstrumentedReadLock.class);
  private transient Timer clock;

  /** Minimum gap between two lock warnings. */
  private final long minLoggingGap;
  /** Threshold for detecting long lock held time. */
  private final long lockWarningThreshold;

  // Tracking counters for lock statistics.
  private volatile long lockAcquireTimestamp;
  private final AtomicLong lastLogTimestamp;
  private final AtomicLong warningsSuppressed = new AtomicLong(0);

  public InstrumentedWriteLock(String name,
      ReentrantReadWriteLock readWriteLock,
      long minLoggingGapMs, long lockWarningThresholdMs) {
    this(name, readWriteLock, minLoggingGapMs, lockWarningThresholdMs,
        new Timer());
  }

  public InstrumentedWriteLock(String name,
      ReentrantReadWriteLock readWriteLock,
      long minLoggingGapMs, long lockWarningThresholdMs, Timer clock) {
    super(readWriteLock);
    this.name = name;
    this.clock = clock;
    minLoggingGap = minLoggingGapMs;
    lockWarningThreshold = lockWarningThresholdMs;
    lastLogTimestamp = new AtomicLong(
        clock.monotonicNow() - Math.max(minLoggingGap, lockWarningThreshold));
    this.writeLock = readWriteLock.writeLock();
  }

  @Override
  public void lock() {
    writeLock.lock();
    lockAcquireTimestamp = clock.monotonicNow();
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    writeLock.lockInterruptibly();
    lockAcquireTimestamp = clock.monotonicNow();
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
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    if (writeLock.tryLock(time, unit)) {
      lockAcquireTimestamp = clock.monotonicNow();
      return true;
    }
    return false;
  }

  @Override
  public void unlock() {
    long localLockReleaseTime = clock.monotonicNow();
    long localLockAcquireTime = lockAcquireTimestamp;
    writeLock.unlock();
    check(localLockAcquireTime, localLockReleaseTime);
  }

  /**
   * Reconstitutes this lock instance from a input stream.
   * @param s the input stream
   */
  private void readObject(java.io.ObjectInputStream s)
      throws ClassNotFoundException, IOException {
    s.defaultReadObject();
    // reset the logger and timer.
    logger = LogFactory.getLog(InstrumentedReadLock.class);
    clock = new Timer();
  }

  /**
   * Logs a warning if the read lock was held for too long.
   *
   * Should be invoked by the caller immediately after releasing the lock.
   *
   */
  protected void check(long acquireTime, long releaseTime) {
    if (!logger.isWarnEnabled()) {
      return;
    }

    final long lockHeldTime = releaseTime - acquireTime;
    if (lockWarningThreshold - lockHeldTime < 0) {
      long now;
      long localLastLogTs;
      do {
        now = clock.monotonicNow();
        localLastLogTs = lastLogTimestamp.get();
        long deltaSinceLastLog = now - localLastLogTs;
        // check should print log or not
        if (deltaSinceLastLog - minLoggingGap < 0) {
          warningsSuppressed.incrementAndGet();
          return;
        }
      } while (!lastLogTimestamp.compareAndSet(localLastLogTs, now));
      long suppressed = warningsSuppressed.getAndSet(0);
      logWarning(lockHeldTime, suppressed);
    }
  }

  protected void logWarning(long lockHeldTime, long suppressed) {
    logger.warn(String.format("Write lock held time above threshold: " +
        "lock identifier: %s " +
        "lockHeldTimeMs=%d ms. Suppressed %d lock warnings. " +
        "The stack trace is: %s" ,
        name, lockHeldTime, suppressed,
        StringUtils.getStackTrace(Thread.currentThread())));
  }
}
