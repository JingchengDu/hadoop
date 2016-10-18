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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;

/**
 * This is a wrap class of a ReentrantReadWriteLock.
 */
public class InstrumentedReadWriteLock implements ReadWriteLock {

  private Lock readLock;
  private Lock writeLock;

  InstrumentedReadWriteLock(boolean fair, String name, Log logger,
      long minLoggingGapMs, long lockWarningThresholdMs) {
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(fair);
    readLock = new InstrumentedReadLock(name, logger, readWriteLock,
        minLoggingGapMs, lockWarningThresholdMs);
    writeLock = new InstrumentedWriteLock(name, logger, readWriteLock,
        minLoggingGapMs, lockWarningThresholdMs);
  }

  @Override
  public Lock readLock() {
    return readLock;
  }

  @Override
  public Lock writeLock() {
    return writeLock;
  }
}
