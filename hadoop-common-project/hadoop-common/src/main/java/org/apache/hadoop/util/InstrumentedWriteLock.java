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

import org.apache.commons.logging.Log;

/**
 * This is a wrap class of a WriteLock.
 */
public class InstrumentedWriteLock extends InstrumentedLock {

  public InstrumentedWriteLock(String name, Log logger,
      ReentrantReadWriteLock readWriteLock,
      long minLoggingGapMs, long lockWarningThresholdMs) {
    this(name, logger, readWriteLock, minLoggingGapMs, lockWarningThresholdMs,
        new Timer());
  }

  public InstrumentedWriteLock(String name, Log logger,
      ReentrantReadWriteLock readWriteLock,
      long minLoggingGapMs, long lockWarningThresholdMs, Timer clock) {
    super(name, logger, readWriteLock.writeLock(), minLoggingGapMs,
        lockWarningThresholdMs, clock);
  }
}
