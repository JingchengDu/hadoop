/*
 *
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
package org.apache.hadoop.hdfs.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

/**
 * Allows multiple concurrent clients to lock on a numeric id with ReentrantReadWriteLock. The
 * intended usage for read lock is as follows:
 *
 * <pre>
 * ReentrantReadWriteLock lock = idReadWriteLock.getLock(id);
 * try {
 *   lock.readLock().lock();
 *   // User code.
 * } finally {
 *   lock.readLock().unlock();
 * }
 * </pre>
 *
 * For write lock, use lock.writeLock()
 */
@InterfaceAudience.Private
public class IdReadWriteLock {
  // The number of lock we want to easily support. It's not a maximum.
  private static final int NB_CONCURRENT_LOCKS = 1000;
  // The pool to get entry from, entries are mapped by weak reference to make it able to be
  // garbage-collected asap
  private final WeakObjectPool<Long, ReentrantReadWriteLock> lockPool =
      new WeakObjectPool<Long, ReentrantReadWriteLock>(
          new WeakObjectPool.ObjectFactory<Long, ReentrantReadWriteLock>() {
            @Override
            public ReentrantReadWriteLock createObject(Long id) {
              return new ReentrantReadWriteLock();
            }
          }, NB_CONCURRENT_LOCKS);
  
  EvictThread evitThread = new EvictThread();
  private int threshold = 1000;

  public IdReadWriteLock(Configuration conf) {
    threshold = conf.getInt("receiver.lockpool.threshold", 1000);
    evitThread.start();
  }
  
  public void shutdown(){
    evitThread.shutdown();
  }
  
  /**
   * Get the ReentrantReadWriteLock corresponding to the given id
   * @param id an arbitrary number to identify the lock
   */
  public ReentrantReadWriteLock getLock(long id) {
    ReentrantReadWriteLock readWriteLock = lockPool.get(id);
    evitThread.evict();
    return readWriteLock;
  }

  private class EvictThread extends Thread {

    private volatile boolean go = true;

    public EvictThread() {
      this.setDaemon(true);
    }

    public void run() {
      while (this.go) {
        synchronized (this) {
          try {
            this.wait(10000); // 10 seconds
          } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
          }
        }
        if (lockPool.size() > threshold) {
          lockPool.purge();
        }
      }
    }

    public void evict() {
      if (lockPool.size() > threshold) {
        synchronized (this) {
          this.notifyAll();
        }
      }
    }

    public synchronized void shutdown() {
      this.go = false;
      this.notifyAll();
    }
  }

  @VisibleForTesting
  int purgeAndGetEntryPoolSize() throws InterruptedException {
    System.gc();
    Thread.sleep(200);
    lockPool.purge();
    return lockPool.size();
  }

  @VisibleForTesting
  public void waitForWaiters(long id, int numWaiters) throws InterruptedException {
    for (ReentrantReadWriteLock readWriteLock;;) {
      readWriteLock = lockPool.get(id);
      if (readWriteLock != null) {
        synchronized (readWriteLock) {
          if (readWriteLock.getQueueLength() >= numWaiters) {
            return;
          }
        }
      }
      Thread.sleep(50);
    }
  }
}