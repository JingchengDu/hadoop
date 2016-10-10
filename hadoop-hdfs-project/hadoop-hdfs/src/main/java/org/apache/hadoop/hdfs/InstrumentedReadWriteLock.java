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

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is a wrap class of a ReentrantReadWriteLock.
 */
public class InstrumentedReadWriteLock extends ReentrantReadWriteLock{

  private static final long serialVersionUID = 1L;
  private InstrumentedReadLock readLock;
  private InstrumentedWriteLock writeLock;

  InstrumentedReadWriteLock(boolean fair, String name,
      ReentrantReadWriteLock readWriteLock,
      long minLoggingGapMs, long lockWarningThresholdMs) {
    super(fair);
    readLock = new InstrumentedReadLock(name, readWriteLock,
        minLoggingGapMs, lockWarningThresholdMs);
    writeLock = new InstrumentedWriteLock(name, readWriteLock,
        minLoggingGapMs, lockWarningThresholdMs);
  }

  @Override
  public ReadLock readLock() {
    return readLock;
  }

  @Override
  public WriteLock writeLock() {
    return writeLock;
  }
}
