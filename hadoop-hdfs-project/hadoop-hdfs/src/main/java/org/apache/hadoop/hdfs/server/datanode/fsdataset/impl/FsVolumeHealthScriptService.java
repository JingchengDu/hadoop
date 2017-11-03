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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

import com.google.common.annotations.VisibleForTesting;

public class FsVolumeHealthScriptService {

  private static Log LOG = LogFactory.getLog(FsVolumeHealthScriptService.class);

  /** Absolute path to the health script. */
  private String volumeHealthScript;
  /** Delay after which disk health script to be executed */
  private long intervalTime;
  /** Time after which the script should be timedout */
  private long scriptTimeout;
  /** Timer used to schedule volume health monitoring script execution */
  private Timer volumeHealthScriptScheduler;

  /** ShellCommandExecutor used to execute monitoring script */
  ShellCommandExecutor shexec = null;

  private long lastReportedTime;

  private TimerTask timer;
  
  /** The map of the disk health status */
  private Map<String, Boolean> volumeHealthStatus = Collections.emptyMap();
  private FsVolumeHealthScriptOutputParser outputParser;

  /**
   * Class which is used by the {@link Timer} class to periodically execute the
   * disk health script.
   * 
   */
  private class VolumeHealthMonitorExecutor extends TimerTask {

    public VolumeHealthMonitorExecutor(String[] args) {
      ArrayList<String> execScript = new ArrayList<String>();
      execScript.add(volumeHealthScript);
      if (args != null) {
        execScript.addAll(Arrays.asList(args));
      }
      shexec = new ShellCommandExecutor(execScript
          .toArray(new String[execScript.size()]), null, null, scriptTimeout);
    }

    @Override
    public void run() {
      boolean finished = false;
      try {
        shexec.execute();
        finished = true;
      } catch (Exception e) {
        // all exceptions are ignored
        LOG.warn("Caught exception : " + e.getMessage());
      } finally {
        lastReportedTime = System.currentTimeMillis();
        if (finished) {
          try {
            reportHealthStatus(shexec.getOutput());
          } catch (IOException e) {
            LOG.warn("Cannot parse the output: " + shexec.getOutput());
          }
        }
        
      }
    }

    public void reportHealthStatus(String output) throws IOException {
      setHealthStatus(outputParser.parse(output));
    }
  }

  /**
   * Gets if the current volume is healthy.
   * @param path The path of the volume.
   * @return False if the status in the cache and it is false.
   */
  public boolean isHealth(String path) {
    // if the status is not there, consider the volume is healthy.
    if (volumeHealthStatus != null && !volumeHealthStatus.isEmpty()) {
      Boolean result = volumeHealthStatus.get(path);
      return result == null ? true : result.booleanValue();
    } else {
      return true;
    }
  }

  public FsVolumeHealthScriptService(String scriptName, long chkInterval, long timeout,
      String[] scriptArgs, FsVolumeHealthScriptOutputParser outputParser) {
    this.lastReportedTime = System.currentTimeMillis();
    this.volumeHealthScript = scriptName;
    this.intervalTime = chkInterval;
    this.scriptTimeout = timeout;
    this.timer = new VolumeHealthMonitorExecutor(scriptArgs);
    this.outputParser = outputParser;
  }

  public long getLastReportedTime() {
    return lastReportedTime;
  }

  public void setHealthStatus(Map<String, Boolean> healthStatus) {
    this.volumeHealthStatus = healthStatus;
  }

  protected void startService() {
    volumeHealthScriptScheduler = new Timer("VolumeHealthMonitor-Timer", true);
    // Start the timer task immediately and
    // then periodically at interval time.
    volumeHealthScriptScheduler.scheduleAtFixedRate(timer, 0, intervalTime);
  }

  protected void stopService() {
    try {
      if (volumeHealthScriptScheduler != null) {
        volumeHealthScriptScheduler.cancel();
      }
      if (shexec != null) {
        Process p = shexec.getProcess();
        if (p != null) {
          p.destroy();
        }
      }
    } catch (Throwable e) {
    }
  }

  /**
   * Determines if the volume health monitoring service should be
   * started. Returns true if following conditions are met:
   * 
   * <ol>
   * <li>Path to the health check script is not empty</li>
   * <li>The health check script file exists</li>
   * </ol>
   * 
   * @return true if the health monitoring service can be started.
   */
  public static boolean shouldRun(String healthScript) {
    if (healthScript == null || healthScript.trim().isEmpty()) {
      return false;
    }
    File f = new File(healthScript);
    return f.exists() && FileUtil.canExecute(f);
  }

  @VisibleForTesting
  void execVolumeHealthMonitor(String[] scriptArgs) {
    VolumeHealthMonitorExecutor monitorExecutor =
        new VolumeHealthMonitorExecutor(scriptArgs);
    monitorExecutor.run();
  }
}
