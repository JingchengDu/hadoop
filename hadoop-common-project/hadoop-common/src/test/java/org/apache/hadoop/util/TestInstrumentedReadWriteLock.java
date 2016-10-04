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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.InstrumentedAutoCloseableReadWriteLockWrapper.InstrumentedAutoCloseableReadLock;
import org.apache.hadoop.util.InstrumentedAutoCloseableReadWriteLockWrapper.InstrumentedAutoCloseableWriteLock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * A test class for InstrumentedAutoCloseableReadLock and
 * InstrumentedAutoCloseableWriteLock.
 */
public class TestInstrumentedReadWriteLock {

  static final Log LOG = LogFactory.getLog(TestInstrumentedReadWriteLock.class);

  @Rule
  public TestName name = new TestName();

  /**
   * Tests exclusive access of the write lock.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testWriteLock() throws Exception {
    String testname = name.getMethodName();
    InstrumentedAutoCloseableReadWriteLockWrapper readWriteLock =
        new InstrumentedAutoCloseableReadWriteLockWrapper(true, testname, LOG,
        0, 300);
    final ThreadLocal<Boolean> locked = new ThreadLocal<Boolean>();
    locked.set(Boolean.FALSE);
    final InstrumentedAutoCloseableWriteLock writeLock =
        readWriteLock.new InstrumentedAutoCloseableWriteLock(
        readWriteLock.getReentrantReadWriteLock()) {
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
    final InstrumentedAutoCloseableReadLock readLock = readWriteLock
        .readLock();
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
    String testname = name.getMethodName();
    InstrumentedAutoCloseableReadWriteLockWrapper readWriteLock =
        new InstrumentedAutoCloseableReadWriteLockWrapper(true, testname, LOG,
        0, 300);
    final InstrumentedAutoCloseableReadLock readLock = readWriteLock
        .readLock();
    final InstrumentedAutoCloseableWriteLock writeLock = readWriteLock
        .writeLock();
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

  /**
   * Tests the warning when the read lock is held longer than threshold.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testReadLockLongHoldingReport() throws Exception {
    String testname = name.getMethodName();
    final AtomicLong time = new AtomicLong(0);
    Timer mclock = new Timer() {
      @Override
      public long monotonicNow() {
        return time.get();
      }
    };

    final AtomicLong wlogged = new AtomicLong(0);
    final AtomicLong wsuppresed = new AtomicLong(0);
    InstrumentedAutoCloseableReadWriteLockWrapper readWriteLock =
        new InstrumentedAutoCloseableReadWriteLockWrapper(
        true, testname, LOG, 2000, 300) {
          @Override
          void logWarning(long lockHeldTime, long suppressed,
              boolean readLock) {
            wlogged.incrementAndGet();
            wsuppresed.set(suppressed);
          }
        };
    InstrumentedAutoCloseableReadLock readLock =
        readWriteLock.new InstrumentedAutoCloseableReadLock(
        readWriteLock.getReentrantReadWriteLock(), mclock);

    readLock.acquire();   // t = 0
    time.set(100);
    readLock.close(); // t = 100
    assertEquals(0, wlogged.get());
    assertEquals(0, wsuppresed.get());

    readLock.acquire();   // t = 100
    time.set(500);
    readLock.close(); // t = 500
    assertEquals(1, wlogged.get());
    assertEquals(0, wsuppresed.get());

    // the suppress counting is only changed when
    // log is needed in the test
    readLock.acquire();   // t = 500
    time.set(900);
    readLock.close(); // t = 900
    assertEquals(1, wlogged.get());
    assertEquals(0, wsuppresed.get());

    readLock.acquire();   // t = 900
    time.set(3000);
    readLock.close(); // t = 3000
    assertEquals(2, wlogged.get());
    assertEquals(1, wsuppresed.get());
  }

  /**
   * Tests the warning when the write lock is held longer than threshold.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testWriteLockLongHoldingReport() throws Exception {
    String testname = name.getMethodName();
    final AtomicLong time = new AtomicLong(0);
    Timer mclock = new Timer() {
      @Override
      public long monotonicNow() {
        return time.get();
      }
    };

    final AtomicLong wlogged = new AtomicLong(0);
    final AtomicLong wsuppresed = new AtomicLong(0);
    InstrumentedAutoCloseableReadWriteLockWrapper readWriteLock =
        new InstrumentedAutoCloseableReadWriteLockWrapper(
        true, testname, LOG, 2000, 300) {
          @Override
          void logWarning(long lockHeldTime, long suppressed,
              boolean readLock) {
            wlogged.incrementAndGet();
            wsuppresed.set(suppressed);
          }
    };
    InstrumentedAutoCloseableWriteLock writeLock =
        readWriteLock.new InstrumentedAutoCloseableWriteLock(
        readWriteLock.getReentrantReadWriteLock(), mclock);

    writeLock.acquire();   // t = 0
    time.set(100);
    writeLock.close(); // t = 100
    assertEquals(0, wlogged.get());
    assertEquals(0, wsuppresed.get());

    writeLock.acquire();   // t = 100
    time.set(500);
    writeLock.close(); // t = 500
    assertEquals(1, wlogged.get());
    assertEquals(0, wsuppresed.get());

    // the suppress counting is only changed when
    // log is needed in the test
    writeLock.acquire();   // t = 500
    time.set(900);
    writeLock.close(); // t = 900
    assertEquals(1, wlogged.get());
    assertEquals(0, wsuppresed.get());

    writeLock.acquire();   // t = 900
    time.set(3000);
    writeLock.close(); // t = 3000
    assertEquals(2, wlogged.get());
    assertEquals(1, wsuppresed.get());
  }
}
