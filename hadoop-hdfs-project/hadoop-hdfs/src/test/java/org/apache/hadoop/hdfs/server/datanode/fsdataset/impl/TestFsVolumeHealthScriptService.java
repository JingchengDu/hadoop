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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFsVolumeHealthScriptService {

  Logger LOG = LoggerFactory.getLogger(TestFsVolumeHealthScriptService.class);
  private static final String BASE_DIR =
      new FileSystemTestHelper().getTestRootDir();
  private static final int NUM_INIT_VOLUMES = 2;

  private Configuration conf;
  private DataNode datanode;
  private DataStorage storage;
  private FsDatasetImpl dataset;
  private String[] initRootDirs;

  private static Storage.StorageDirectory createStorageDirectory(File root) {
    Storage.StorageDirectory sd = new Storage.StorageDirectory(root);
    DataStorage.createStorageID(sd, false);
    return sd;
  }

  private static void createStorageDirs(DataStorage storage, Configuration conf,
      int numDirs) throws IOException {
    List<Storage.StorageDirectory> dirs =
        new ArrayList<Storage.StorageDirectory>();
    List<String> dirStrings = new ArrayList<String>();
    FileUtils.deleteDirectory(new File(BASE_DIR));
    for (int i = 0; i < numDirs; i++) {
      File loc = new File(BASE_DIR + "/data" + i);
      dirStrings.add(new Path(loc.toString()).toUri().toString());
      loc.mkdirs();
      dirs.add(createStorageDirectory(loc));
      when(storage.getStorageDir(i)).thenReturn(dirs.get(i));
    }

    String dataDir = StringUtils.join(",", dirStrings);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDir);
    when(storage.dirIterator()).thenReturn(dirs.iterator());
    when(storage.getNumStorageDirs()).thenReturn(numDirs);
  }

  private int getNumVolumes() {
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      return volumes.size();
    } catch (IOException e) {
      return 0;
    }
  }

  @Before
  public void setUp() throws IOException {
    datanode = mock(DataNode.class);
    storage = mock(DataStorage.class);
    this.conf = new Configuration();
    this.conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 0);
    this.conf.set(DFSConfigKeys.VOLUME_HEALTH_CHECK_SCRIPT_OUTPUT_PARSER,
      DummyFsVolumeHealthScriptOutputParser.class.getName());
    File scriptLocation =
        new File("target/test-classes/dummy-fsvolume-health-script.sh");
    scriptLocation.setExecutable(true);
    this.conf.set(DFSConfigKeys.VOLUME_HEALTH_CHECK_SCRIPT_PATH,
      scriptLocation.getAbsolutePath());
    final DNConf dnConf = new DNConf(conf);

    when(datanode.getConf()).thenReturn(conf);
    when(datanode.getDnConf()).thenReturn(dnConf);
    final BlockScanner disabledBlockScanner = new BlockScanner(datanode, conf);
    when(datanode.getBlockScanner()).thenReturn(disabledBlockScanner);
    final ShortCircuitRegistry shortCircuitRegistry =
        new ShortCircuitRegistry(conf);
    when(datanode.getShortCircuitRegistry()).thenReturn(shortCircuitRegistry);

    createStorageDirs(storage, conf, NUM_INIT_VOLUMES);
    initRootDirs = new String[storage.getNumStorageDirs()];
    for (int i = 0; i < storage.getNumStorageDirs(); i++) {
      initRootDirs[i] = storage.getStorageDir(i).getRoot().getAbsolutePath();
    }
    this.conf.setStrings(
      DFSConfigKeys.VOLUME_HEALTH_CHECK_SCRIPT_OUTPUT_PARSER_ARGS,
      initRootDirs);
    dataset = new FsDatasetImpl(datanode, storage, conf);

    assertEquals(NUM_INIT_VOLUMES, getNumVolumes());
    assertEquals(0, dataset.getNumFailedVolumes());
  }

  @Test
  public void testVolumeHealthScriptService() throws Exception {
    dataset.getFsVolumeHealthScriptService().execVolumeHealthMonitor(null);
    int index = 0;
    for(String rootDir : initRootDirs) {
      if (index == 0) {
        assertFalse(dataset.isHealth(rootDir));
        index++;
      } else {
        assertTrue(dataset.isHealth(rootDir));
      }
    }
  }

  public static class DummyFsVolumeHealthScriptOutputParser
      implements FsVolumeHealthScriptOutputParser {
    private String[] existingRootDirs = null;

    public DummyFsVolumeHealthScriptOutputParser(String[] existingRootDirs) {
      this.existingRootDirs = existingRootDirs;
    }

    @Override
    public Map<String, Boolean> parse(String output) throws IOException {
      Map<String, Boolean> status = new HashMap<>();
      status.put(existingRootDirs[0], Boolean.FALSE);
      return status;
    }

  }
}
