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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class IdLockPool {
  static final Log LOG = LogFactory.getLog(IdLockPool.class);
  private final WeakObjectPool<Long, Object> lockPool;
  
  EvictThread evitThread = new EvictThread();
  private long sleepInterval = 10000;  //10 seconds
  private int threshold = 150;
  private int concurrencyLevel = 128;

  public IdLockPool(Configuration conf) {
    threshold = conf.getInt("dfs.lock.pool.clean.threshold", threshold);
    sleepInterval = conf.getLong("dfs.lock.pool.clean.interval", sleepInterval);
    concurrencyLevel = conf.getInt("dfs.lock.pool.concurrencyLevel", concurrencyLevel);
    lockPool = new WeakObjectPool<Long, Object>(
      new WeakObjectPool.ObjectFactory<Long, Object>() {
        @Override
        public Object createObject(Long id) {
          return new Object();
        }
      }, 1280, concurrencyLevel);
    evitThread.start();
  }
  
  public void shutdown(){
    evitThread.shutdown();
  }
  
  /**
   * Get the ReentrantReadWriteLock corresponding to the given id
   * @param id an arbitrary number to identify the lock
   */
  public Object getLock(long id) {
    Object readWriteLock = lockPool.get(id);
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
            this.wait(sleepInterval);
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
}