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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

/**
 * This is a debugging class that can be used by callers to track
 * whether a specifc lock is being held for too long and periodically
 * log a warning and stack trace, if so.
 *
 * The logged warnings are throttled so that logs are not spammed.
 *
 * A new instance of InstrumentedWriteLock can be created for each object
 * that needs to be instrumented.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InstrumentedWriteLock extends WriteLock {

  private static final long serialVersionUID = 1L;
  private final Log logger;
  private final String name;
  private final transient Timer clock;

  /** Minimum gap between two lock warnings. */
  private final long minLoggingGap;
  /** Threshold for detecting long lock held time. */
  private final long lockWarningThreshold;

  // Tracking counters for the read lock statistics.
  private volatile long lockAcquireTimestamp;
  private final AtomicLong lastLogTimestamp;
  private final AtomicLong warningsSuppressed = new AtomicLong(0);

  /**
   * Create a instrumented write lock instance which logs a warning message
   * when lock held time is above given threshold.
   */
  protected InstrumentedWriteLock(ReentrantReadWriteLock lock, String name,
      Log logger, long minLoggingGapMs, long lockWarningThresholdMs) {
    this(lock, name, logger, minLoggingGapMs, lockWarningThresholdMs,
        new Timer());
  }

  @VisibleForTesting
  InstrumentedWriteLock(ReentrantReadWriteLock lock, String name, Log logger,
      long minLoggingGapMs, long lockWarningThresholdMs, Timer clock) {
    super(lock);
    this.name = name;
    this.clock = clock;
    this.logger = logger;
    minLoggingGap = minLoggingGapMs;
    lockWarningThreshold = lockWarningThresholdMs;
    lastLogTimestamp = new AtomicLong(clock.monotonicNow()
        - Math.max(minLoggingGap, lockWarningThreshold));
  }

  @Override
  public void lock() {
    super.lock();
    lockAcquireTimestamp = clock.monotonicNow();
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    super.lockInterruptibly();
    lockAcquireTimestamp = clock.monotonicNow();
  }

  @Override
  public boolean tryLock() {
    if (super.tryLock()) {
      lockAcquireTimestamp = clock.monotonicNow();
      return true;
    }
    return false;
  }

  @Override
  public boolean tryLock(long timeout, TimeUnit unit)
      throws InterruptedException {
    if (super.tryLock(timeout, unit)) {
      lockAcquireTimestamp = clock.monotonicNow();
      return true;
    }
    return false;
  }

  @Override
  public void unlock() {
    long localLockReleaseTime = clock.monotonicNow();
    long localLockAcquireTime = lockAcquireTimestamp;
    super.unlock();
    check(localLockAcquireTime, localLockReleaseTime);
  }

  @VisibleForTesting
  void logWarning(long lockHeldTime, long suppressed) {
    logger.warn(String.format("Write lock held time above threshold: " +
        "lock identifier: %s " +
        "lockHeldTimeMs=%d ms. Suppressed %d lock warnings. " +
        "The stack trace is: %s" ,
        name, lockHeldTime, suppressed,
        StringUtils.getStackTrace(Thread.currentThread())));
  }

  /**
   * Logs a warning if the lock was held for too long.
   *
   * Should be invoked by the caller immediately AFTER releasing the lock.
   *
   * @param acquireTime  - timestamp just after acquiring the lock.
   * @param releaseTime - timestamp just before releasing the lock.
   */
  private void check(long acquireTime, long releaseTime) {
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
}
