package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaUnderRecovery;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaTracker.RamDiskReplica;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.Time;

public class NewFsDatasetImpl extends FsDatasetImpl {
  static final Log LOG = LogFactory.getLog(NewFsDatasetImpl.class);
  private Map<Integer, Object> lockPool;
  private int lockPoolCapacity = 128;

  NewFsDatasetImpl(DataNode datanode, DataStorage storage, Configuration conf) throws IOException {
    super(datanode, storage, conf);
    lockPoolCapacity = conf.getInt("dataset.lockpool.capacity", 128);
    lockPool = new HashMap<Integer, Object>(lockPoolCapacity);
    for (int i = 0; i < lockPoolCapacity; i++) {
      lockPool.put(Integer.valueOf(i), new Object());
    }
  }

  private Object getLock(long blockId) {
    return lockPool.get(Integer.valueOf((int) (blockId % lockPoolCapacity)));
  }

  @Override
  public FsVolumeImpl getVolume(ExtendedBlock b) {
    // TODO consider if we need to sync this method
    final ReplicaInfo r =  volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    return r != null? (FsVolumeImpl)r.getVolume(): null;
  }

  @Override
  public void checkAndUpdate(String bpid, long blockId, File diskFile, File diskMetaFile,
    FsVolumeSpi vol) throws IOException {
    Block corruptBlock = null;
    ReplicaInfo memBlockInfo;
    synchronized (getLock(blockId)) {
      synchronized (this) {
        memBlockInfo = volumeMap.get(bpid, blockId);
        if (memBlockInfo != null && memBlockInfo.getState() != ReplicaState.FINALIZED) {
          // Block is not finalized - ignore the difference
          return;
        }

        final long diskGS = diskMetaFile != null && diskMetaFile.exists() ?
            Block.getGenerationStamp(diskMetaFile.getName()) :
              HdfsConstants.GRANDFATHER_GENERATION_STAMP;

        if (diskFile == null || !diskFile.exists()) {
          if (memBlockInfo == null) {
            // Block file does not exist and block does not exist in memory
            // If metadata file exists then delete it
            if (diskMetaFile != null && diskMetaFile.exists()
                && diskMetaFile.delete()) {
              LOG.warn("Deleted a metadata file without a block "
                  + diskMetaFile.getAbsolutePath());
            }
            return;
          }
          if (!memBlockInfo.getBlockFile().exists()) {
            // Block is in memory and not on the disk
            // Remove the block from volumeMap
            volumeMap.remove(bpid, blockId);
            if (vol.isTransientStorage()) {
              ramDiskReplicaTracker.discardReplica(bpid, blockId, true);
            }
            LOG.warn("Removed block " + blockId
                + " from memory with missing block file on the disk");
            // Finally remove the metadata file
            if (diskMetaFile != null && diskMetaFile.exists()
                && diskMetaFile.delete()) {
              LOG.warn("Deleted a metadata file for the deleted block "
                  + diskMetaFile.getAbsolutePath());
            }
          }
          return;
        }
        /*
         * Block file exists on the disk
         */
        if (memBlockInfo == null) {
          // Block is missing in memory - add the block to volumeMap
          ReplicaInfo diskBlockInfo = new FinalizedReplica(blockId, 
              diskFile.length(), diskGS, vol, diskFile.getParentFile());
          volumeMap.add(bpid, diskBlockInfo);
          if (vol.isTransientStorage()) {
            long lockedBytesReserved =
                cacheManager.reserve(diskBlockInfo.getNumBytes()) > 0 ?
                    diskBlockInfo.getNumBytes() : 0;
            ramDiskReplicaTracker.addReplica(
                bpid, blockId, (FsVolumeImpl) vol, lockedBytesReserved);
          }
          LOG.warn("Added missing block to memory " + diskBlockInfo);
          return;
        }
        /*
         * Block exists in volumeMap and the block file exists on the disk
         */
        // Compare block files
        File memFile = memBlockInfo.getBlockFile();
        if (memFile.exists()) {
          if (memFile.compareTo(diskFile) != 0) {
            if (diskMetaFile.exists()) {
              if (memBlockInfo.getMetaFile().exists()) {
                // We have two sets of block+meta files. Decide which one to
                // keep.
                ReplicaInfo diskBlockInfo = new FinalizedReplica(
                    blockId, diskFile.length(), diskGS, vol, diskFile.getParentFile());
                ((FsVolumeImpl) vol).getBlockPoolSlice(bpid).resolveDuplicateReplicas(
                    memBlockInfo, diskBlockInfo, volumeMap);
              }
            } else {
              if (!diskFile.delete()) {
                LOG.warn("Failed to delete " + diskFile + ". Will retry on next scan");
              }
            }
          }
        } else {
          // Block refers to a block file that does not exist.
          // Update the block with the file found on the disk. Since the block
          // file and metadata file are found as a pair on the disk, update
          // the block based on the metadata file found on the disk
          LOG.warn("Block file in volumeMap "
              + memFile.getAbsolutePath()
              + " does not exist. Updating it to the file found during scan "
              + diskFile.getAbsolutePath());
          memBlockInfo.setDir(diskFile.getParentFile());
          memFile = diskFile;

          LOG.warn("Updating generation stamp for block " + blockId
              + " from " + memBlockInfo.getGenerationStamp() + " to " + diskGS);
          memBlockInfo.setGenerationStamp(diskGS);
        }

        // Compare generation stamp
        if (memBlockInfo.getGenerationStamp() != diskGS) {
          File memMetaFile = FsDatasetUtil.getMetaFile(diskFile, 
              memBlockInfo.getGenerationStamp());
          if (memMetaFile.exists()) {
            if (memMetaFile.compareTo(diskMetaFile) != 0) {
              LOG.warn("Metadata file in memory "
                  + memMetaFile.getAbsolutePath()
                  + " does not match file found by scan "
                  + (diskMetaFile == null? null: diskMetaFile.getAbsolutePath()));
            }
          } else {
            // Metadata file corresponding to block in memory is missing
            // If metadata file found during the scan is on the same directory
            // as the block file, then use the generation stamp from it
            long gs = diskMetaFile != null && diskMetaFile.exists()
                && diskMetaFile.getParent().equals(memFile.getParent()) ? diskGS
                : HdfsConstants.GRANDFATHER_GENERATION_STAMP;

            LOG.warn("Updating generation stamp for block " + blockId
                + " from " + memBlockInfo.getGenerationStamp() + " to " + gs);

            memBlockInfo.setGenerationStamp(gs);
          }
        }

        // Compare block size
        if (memBlockInfo.getNumBytes() != memFile.length()) {
          // Update the length based on the block file
          corruptBlock = new Block(memBlockInfo);
          LOG.warn("Updating size of block " + blockId + " from "
              + memBlockInfo.getNumBytes() + " to " + memFile.length());
          memBlockInfo.setNumBytes(memFile.length());
        }
      } 
    }
    // Send corrupt block report outside the lock
    if (corruptBlock != null) {
      LOG.warn("Reporting the block " + corruptBlock
          + " as corrupt due to length mismatch");
      try {
        datanode.reportBadBlocks(new ExtendedBlock(bpid, corruptBlock));  
      } catch (IOException e) {
        LOG.warn("Failed to repot bad block " + corruptBlock, e);
      }
    }
  }

  @Override
  public Block getStoredBlock(String bpid, long blkid) throws IOException {
    synchronized (getLock(blkid)) {
      File blockfile = getFile(bpid, blkid, false);
      if (blockfile == null) {
        return null;
      }
      final File metafile = FsDatasetUtil.findMetaFile(blockfile);
      final long gs = FsDatasetUtil.parseGenerationStamp(blockfile, metafile);
      return new Block(blkid, blockfile.length(), gs);
    }
  }

  @Override
  public ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkOffset, long metaOffset)
    throws IOException {
    synchronized (getLock(b.getBlockId())) {
      ReplicaInfo info = getReplicaInfo(b);
      FsVolumeReference ref = info.getVolume().obtainReference();
      try {
        InputStream blockInStream = openAndSeek(info.getBlockFile(), blkOffset);
        try {
          InputStream metaInStream = openAndSeek(info.getMetaFile(), metaOffset);
          return new ReplicaInputStreams(blockInStream, metaInStream, ref);
        } catch (IOException e) {
          IOUtils.cleanup(null, blockInStream);
          throw e;
        }
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
    }
  }

  @Override
  public ReplicaHandler createTemporary(StorageType storageType, ExtendedBlock b)
    throws IOException {
    long startTimeMs = Time.monotonicNow();
    long writerStopTimeoutMs = datanode.getDnConf().getXceiverStopTimeout();
    ReplicaInfo lastFoundReplicaInfo = null;
    do {
      synchronized (getLock(b.getBlockId())) {
        ReplicaInfo currentReplicaInfo = volumeMap.get(b.getBlockPoolId(), b.getBlockId());
        if (currentReplicaInfo == lastFoundReplicaInfo) {
          if (lastFoundReplicaInfo != null) {
            invalidate(b.getBlockPoolId(), new Block[] { lastFoundReplicaInfo });
          }
          FsVolumeReference ref = volumes.getNextVolume(storageType, b.getNumBytes());
          FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();
          // create a temporary file to hold block in the designated volume
          File f;
          try {
            f = v.createTmpFile(b.getBlockPoolId(), b.getLocalBlock());
          } catch (IOException e) {
            IOUtils.cleanup(null, ref);
            throw e;
          }
          ReplicaInPipeline newReplicaInfo = new ReplicaInPipeline(b.getBlockId(),
            b.getGenerationStamp(), v, f.getParentFile(), b.getLocalBlock().getNumBytes());
          synchronized (this) {
            volumeMap.add(b.getBlockPoolId(), newReplicaInfo);
          }
          return new ReplicaHandler(newReplicaInfo, ref);
        } else {
          if (!(currentReplicaInfo.getGenerationStamp() < b.
            getGenerationStamp() && currentReplicaInfo instanceof ReplicaInPipeline)) {
            throw new ReplicaAlreadyExistsException("Block " + b + " already exists in state "
              + currentReplicaInfo.getState() + " and thus cannot be created.");
          }
          lastFoundReplicaInfo = currentReplicaInfo;
        }
      }

      // Hang too long, just bail out. This is not supposed to happen.
      long writerStopMs = Time.monotonicNow() - startTimeMs;
      if (writerStopMs > writerStopTimeoutMs) {
        LOG.warn("Unable to stop existing writer for block " + b + " after " 
            + writerStopMs + " miniseconds.");
        throw new IOException("Unable to stop existing writer for block " + b
            + " after " + writerStopMs + " miniseconds.");
      }

      // Stop the previous writer
      ((ReplicaInPipeline) lastFoundReplicaInfo)
          .stopWriter(writerStopTimeoutMs);
    } while (true);
  }

  @Override
  public ReplicaHandler createRbw(StorageType storageType, ExtendedBlock b, boolean allowLazyPersist)
    throws IOException {
    synchronized (getLock(b.getBlockId())) {
      ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getBlockId());
      if (replicaInfo != null) {
        throw new ReplicaAlreadyExistsException("Block " + b + " already exists in state "
          + replicaInfo.getState() + " and thus cannot be created.");
      }
      // create a new block
      FsVolumeReference ref = null;

      // Use ramdisk only if block size is a multiple of OS page size.
      // This simplifies reservation for partially used replicas
      // significantly.
      if (allowLazyPersist && lazyWriter != null
        && b.getNumBytes() % cacheManager.getOsPageSize() == 0
        && reserveLockedMemory(b.getNumBytes())) {
        try {
          // First try to place the block on a transient volume.
          ref = volumes.getNextTransientVolume(b.getNumBytes());
          datanode.getMetrics().incrRamDiskBlocksWrite();
        } catch (DiskOutOfSpaceException de) {
          // Ignore the exception since we just fall back to persistent storage.
        } finally {
          if (ref == null) {
            cacheManager.release(b.getNumBytes());
          }
        }
      }

      if (ref == null) {
        ref = volumes.getNextVolume(storageType, b.getNumBytes());
      }

      FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();
      // create an rbw file to hold block in the designated volume

      if (allowLazyPersist && !v.isTransientStorage()) {
        datanode.getMetrics().incrRamDiskBlocksWriteFallback();
      }

      File f;
      try {
        f = v.createRbwFile(b.getBlockPoolId(), b.getLocalBlock());
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }

      ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(b.getBlockId(),
        b.getGenerationStamp(), v, f.getParentFile(), b.getNumBytes());
      synchronized (this) {
        volumeMap.add(b.getBlockPoolId(), newReplicaInfo);
      }
      return new ReplicaHandler(newReplicaInfo, ref);
    }
  }

  @Override
  public ReplicaHandler recoverRbw(ExtendedBlock b, long newGS, long minBytesRcvd, long maxBytesRcvd)
    throws IOException {
    LOG.info("Recover RBW replica " + b);
    synchronized (getLock(b.getBlockId())) {
      ReplicaInfo replicaInfo = getReplicaInfo(b.getBlockPoolId(), b.getBlockId());
      
      // check the replica's state
      if (replicaInfo.getState() != ReplicaState.RBW) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_RBW_REPLICA + replicaInfo);
      }
      ReplicaBeingWritten rbw = (ReplicaBeingWritten)replicaInfo;
      
      LOG.info("Recovering " + rbw);

      // Stop the previous writer
      rbw.stopWriter(datanode.getDnConf().getXceiverStopTimeout());
      rbw.setWriter(Thread.currentThread());

      // check generation stamp
      long replicaGenerationStamp = rbw.getGenerationStamp();
      if (replicaGenerationStamp < b.getGenerationStamp() ||
          replicaGenerationStamp > newGS) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + b +
            ". Expected GS range is [" + b.getGenerationStamp() + ", " + 
            newGS + "].");
      }
      
      // check replica length
      long bytesAcked = rbw.getBytesAcked();
      long numBytes = rbw.getNumBytes();
      if (bytesAcked < minBytesRcvd || numBytes > maxBytesRcvd){
        throw new ReplicaNotFoundException("Unmatched length replica " + 
            replicaInfo + ": BytesAcked = " + bytesAcked + 
            " BytesRcvd = " + numBytes + " are not in the range of [" + 
            minBytesRcvd + ", " + maxBytesRcvd + "].");
      }

      FsVolumeReference ref = rbw.getVolume().obtainReference();
      try {
        // Truncate the potentially corrupt portion.
        // If the source was client and the last node in the pipeline was lost,
        // any corrupt data written after the acked length can go unnoticed.
        if (numBytes > bytesAcked) {
          final File replicafile = rbw.getBlockFile();
          truncateBlock(replicafile, rbw.getMetaFile(), numBytes, bytesAcked);
          rbw.setNumBytes(bytesAcked);
          rbw.setLastChecksumAndDataLen(bytesAcked, null);
        }

        // bump the replica's generation stamp to newGS
        bumpReplicaGS(rbw, newGS);
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
      return new ReplicaHandler(rbw, ref);
    }
  }

  @Override
  public ReplicaInPipeline convertTemporaryToRbw(ExtendedBlock b)
    throws IOException {
    synchronized (getLock(b.getBlockId())) {
      final long blockId = b.getBlockId();
      final long expectedGs = b.getGenerationStamp();
      final long visible = b.getNumBytes();
      LOG.info("Convert " + b + " from Temporary to RBW, visible length="
          + visible);

      final ReplicaInPipeline temp;
      {
        // get replica
        final ReplicaInfo r = volumeMap.get(b.getBlockPoolId(), blockId);
        if (r == null) {
          throw new ReplicaNotFoundException(
              ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
        }
        // check the replica's state
        if (r.getState() != ReplicaState.TEMPORARY) {
          throw new ReplicaAlreadyExistsException(
              "r.getState() != ReplicaState.TEMPORARY, r=" + r);
        }
        temp = (ReplicaInPipeline)r;
      }
      // check generation stamp
      if (temp.getGenerationStamp() != expectedGs) {
        throw new ReplicaAlreadyExistsException(
            "temp.getGenerationStamp() != expectedGs = " + expectedGs
            + ", temp=" + temp);
      }

      // TODO: check writer?
      // set writer to the current thread
      // temp.setWriter(Thread.currentThread());

      // check length
      final long numBytes = temp.getNumBytes();
      if (numBytes < visible) {
        throw new IOException(numBytes + " = numBytes < visible = "
            + visible + ", temp=" + temp);
      }
      // check volume
      final FsVolumeImpl v = (FsVolumeImpl)temp.getVolume();
      if (v == null) {
        throw new IOException("r.getVolume() = null, temp="  + temp);
      }
      
      // move block files to the rbw directory
      BlockPoolSlice bpslice = v.getBlockPoolSlice(b.getBlockPoolId());
      final File dest = moveBlockFiles(b.getLocalBlock(), temp.getBlockFile(), 
          bpslice.getRbwDir());
      // create RBW
      final ReplicaBeingWritten rbw = new ReplicaBeingWritten(
          blockId, numBytes, expectedGs,
          v, dest.getParentFile(), Thread.currentThread(), 0);
      rbw.setBytesAcked(visible);
      // overwrite the RBW in the volume map
      synchronized (this) {
        // TODO it's better to have a concurrent map
        volumeMap.add(b.getBlockPoolId(), rbw);
      }
      return rbw;
    }
  }

  @Override
  public ReplicaHandler append(ExtendedBlock b, long newGS, long expectedBlockLen)
    throws IOException {
    if (newGS < b.getGenerationStamp()) {
      throw new IOException("The new generation stamp " + newGS + 
          " should be greater than the replica " + b + "'s generation stamp");
    }
    synchronized (getLock(b.getBlockId())) {
      ReplicaInfo replicaInfo = getReplicaInfo(b);
      LOG.info("Appending to " + replicaInfo);
      if (replicaInfo.getState() != ReplicaState.FINALIZED) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.UNFINALIZED_REPLICA + b);
      }
      if (replicaInfo.getNumBytes() != expectedBlockLen) {
        throw new IOException("Corrupted replica " + replicaInfo + 
            " with a length of " + replicaInfo.getNumBytes() + 
            " expected length is " + expectedBlockLen);
      }

      FsVolumeReference ref = replicaInfo.getVolume().obtainReference();
      ReplicaBeingWritten replica = null;
      try {
        replica = append(b.getBlockPoolId(), (FinalizedReplica)replicaInfo, newGS,
            b.getNumBytes());
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
      return new ReplicaHandler(replica, ref);
    }
  }

  private ReplicaBeingWritten append(String bpid, FinalizedReplica replicaInfo,
    long newGS, long estimateBlockLen) throws IOException {
    // If the block is cached, start uncaching it.
    cacheManager.uncacheBlock(bpid, replicaInfo.getBlockId());

    // construct a RBW replica with the new GS
    File blkfile = replicaInfo.getBlockFile();
    FsVolumeImpl v = (FsVolumeImpl) replicaInfo.getVolume();
    if (v.getAvailable() < estimateBlockLen - replicaInfo.getNumBytes()) {
      throw new DiskOutOfSpaceException("Insufficient space for appending to " + replicaInfo);
    }
    File newBlkFile = new File(v.getRbwDir(bpid), replicaInfo.getBlockName());
    File oldmeta = replicaInfo.getMetaFile();
    ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(replicaInfo.getBlockId(),
      replicaInfo.getNumBytes(), newGS, v, newBlkFile.getParentFile(), Thread.currentThread(),
      estimateBlockLen);
    File newmeta = newReplicaInfo.getMetaFile();

    // rename meta file to rbw directory
    if (LOG.isDebugEnabled()) {
      LOG.debug("Renaming " + oldmeta + " to " + newmeta);
    }
    try {
      NativeIO.renameTo(oldmeta, newmeta);
    } catch (IOException e) {
      throw new IOException("Block " + replicaInfo + " reopen failed. "
        + " Unable to move meta file  " + oldmeta + " to rbw dir " + newmeta, e);
    }

    // rename block file to rbw directory
    if (LOG.isDebugEnabled()) {
      LOG.debug("Renaming " + blkfile + " to " + newBlkFile + ", file length=" + blkfile.length());
    }
    try {
      NativeIO.renameTo(blkfile, newBlkFile);
    } catch (IOException e) {
      try {
        NativeIO.renameTo(newmeta, oldmeta);
      } catch (IOException ex) {
        LOG.warn("Cannot move meta file " + newmeta + "back to the finalized directory " + oldmeta,
          ex);
      }
      throw new IOException("Block " + replicaInfo + " reopen failed. "
        + " Unable to move block file " + blkfile + " to rbw dir " + newBlkFile, e);
    }

    // Replace finalized replica by a RBW replica in replicas map
    synchronized (this) {
      volumeMap.add(bpid, newReplicaInfo);
    }
    v.reserveSpaceForReplica(estimateBlockLen - replicaInfo.getNumBytes());
    return newReplicaInfo;
  }

  @Override
  public ReplicaHandler recoverAppend(ExtendedBlock b, long newGS, long expectedBlockLen)
    throws IOException {
    LOG.info("Recover failed append to " + b);

    synchronized (getLock(b.getBlockId())) {
      ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);

      FsVolumeReference ref = replicaInfo.getVolume().obtainReference();
      ReplicaBeingWritten replica;
      try {
        // change the replica's state/gs etc.
        if (replicaInfo.getState() == ReplicaState.FINALIZED) {
          replica = append(b.getBlockPoolId(), (FinalizedReplica) replicaInfo,
                           newGS, b.getNumBytes());
        } else { //RBW
          bumpReplicaGS(replicaInfo, newGS);
          replica = (ReplicaBeingWritten) replicaInfo;
        }
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
      return new ReplicaHandler(replica, ref);
    }
  }

  @Override
  public String recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException {
    LOG.info("Recover failed close " + b);
    synchronized (getLock(b.getBlockId())) {
      // check replica's state
      ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);
      // bump the replica's GS
      bumpReplicaGS(replicaInfo, newGS);
      // finalize the replica if RBW
      if (replicaInfo.getState() == ReplicaState.RBW) {
        finalizeReplica(b.getBlockPoolId(), replicaInfo);
      }
      return replicaInfo.getStorageUuid();
    }
  }

  private FinalizedReplica finalizeReplica(String bpid,
      ReplicaInfo replicaInfo) throws IOException {
    FinalizedReplica newReplicaInfo = null;
    if (replicaInfo.getState() == ReplicaState.RUR &&
       ((ReplicaUnderRecovery)replicaInfo).getOriginalReplica().getState() == 
         ReplicaState.FINALIZED) {
      newReplicaInfo = (FinalizedReplica)
             ((ReplicaUnderRecovery)replicaInfo).getOriginalReplica();
    } else {
      FsVolumeImpl v = (FsVolumeImpl)replicaInfo.getVolume();
      File f = replicaInfo.getBlockFile();
      if (v == null) {
        throw new IOException("No volume for temporary file " + f + 
            " for block " + replicaInfo);
      }

      File dest = v.addFinalizedBlock(
          bpid, replicaInfo, f, replicaInfo.getBytesReserved());
      newReplicaInfo = new FinalizedReplica(replicaInfo, v, dest.getParentFile());

      if (v.isTransientStorage()) {
        releaseLockedMemory(
            replicaInfo.getOriginalBytesReserved() - replicaInfo.getNumBytes(),
            false);
        ramDiskReplicaTracker.addReplica(
            bpid, replicaInfo.getBlockId(), v, replicaInfo.getNumBytes());
        datanode.getMetrics().addRamDiskBytesWrite(replicaInfo.getNumBytes());
      }
    }
    synchronized (this) {
      volumeMap.add(bpid, newReplicaInfo);
    }

    return newReplicaInfo;
  }

  @Override
  public void finalizeBlock(ExtendedBlock b) throws IOException {
    if (Thread.interrupted()) {
      // Don't allow data modifications from interrupted threads
      throw new IOException("Cannot finalize block from Interrupted Thread");
    }
    synchronized (getLock(b.getBlockId())) {
      ReplicaInfo replicaInfo = getReplicaInfo(b);
      if (replicaInfo.getState() == ReplicaState.FINALIZED) {
        // this is legal, when recovery happens on a file that has
        // been opened for append but never modified
        return;
      }
      finalizeReplica(b.getBlockPoolId(), replicaInfo);
    }
  }

  @Override
  public void unfinalizeBlock(ExtendedBlock b) throws IOException {
    synchronized (getLock(b.getBlockId())) {
      ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
      if (replicaInfo != null && replicaInfo.getState() == ReplicaState.TEMPORARY) {
        // remove from volumeMap
        volumeMap.remove(b.getBlockPoolId(), b.getLocalBlock());

        // delete the on-disk temp file
        if (delBlockFromDisk(replicaInfo.getBlockFile(), replicaInfo.getMetaFile(),
          b.getLocalBlock())) {
          LOG.warn("Block " + b + " unfinalized and removed. ");
        }
        if (replicaInfo.getVolume().isTransientStorage()) {
          ramDiskReplicaTracker.discardReplica(b.getBlockPoolId(), b.getBlockId(), true);
        }
      }
    }
  }

  @Override
  public boolean contains(ExtendedBlock block) {
    synchronized (getLock(block.getBlockId())) {
      final long blockId = block.getLocalBlock().getBlockId();
      return getFile(block.getBlockPoolId(), blockId, false) != null;
    }
  }

  @Override
  public void invalidate(String bpid, Block[] invalidBlks) throws IOException {
    final List<String> errors = new ArrayList<String>();
    for (int i = 0; i < invalidBlks.length; i++) {
      final File f;
      final FsVolumeImpl v;
      synchronized (getLock(invalidBlks[i].getBlockId())) {
        final ReplicaInfo info = volumeMap.get(bpid, invalidBlks[i]);
        if (info == null) {
          // It is okay if the block is not found -- it may be deleted earlier.
          LOG.info("Failed to delete replica " + invalidBlks[i]
              + ": ReplicaInfo not found.");
          continue;
        }
        if (info.getGenerationStamp() != invalidBlks[i].getGenerationStamp()) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              + ": GenerationStamp not matched, info=" + info);
          continue;
        }
        f = info.getBlockFile();
        v = (FsVolumeImpl)info.getVolume();
        if (v == null) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              +  ". No volume for this replica, file=" + f);
          continue;
        }
        File parent = f.getParentFile();
        if (parent == null) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              +  ". Parent not found for file " + f);
          continue;
        }
        synchronized (this) {
          ReplicaInfo removing = volumeMap.remove(bpid, invalidBlks[i]);
          addDeletingBlock(bpid, removing.getBlockId());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Block file " + removing.getBlockFile().getName()
                + " is to be deleted");
          }
        }
      }

      if (v.isTransientStorage()) {
        RamDiskReplica replicaInfo =
          ramDiskReplicaTracker.getReplica(bpid, invalidBlks[i].getBlockId());
        if (replicaInfo != null) {
          if (!replicaInfo.getIsPersisted()) {
            datanode.getMetrics().incrRamDiskBlocksDeletedBeforeLazyPersisted();
          }
          ramDiskReplicaTracker.discardReplica(replicaInfo.getBlockPoolId(),
            replicaInfo.getBlockId(), true);
        }
      }

      // If a DFSClient has the replica in its cache of short-circuit file
      // descriptors (and the client is using ShortCircuitShm), invalidate it.
      datanode.getShortCircuitRegistry().processBlockInvalidation(
                new ExtendedBlockId(invalidBlks[i].getBlockId(), bpid));

      // If the block is cached, start uncaching it.
      cacheManager.uncacheBlock(bpid, invalidBlks[i].getBlockId());

      // Delete the block asynchronously to make sure we can do it fast enough.
      // It's ok to unlink the block file before the uncache operation
      // finishes.
      try {
        asyncDiskService.deleteAsync(v.obtainReference(), f,
            FsDatasetUtil.getMetaFile(f, invalidBlks[i].getGenerationStamp()),
            new ExtendedBlock(bpid, invalidBlks[i]),
            dataStorage.getTrashDirectoryForBlockFile(bpid, f));
      } catch (ClosedChannelException e) {
        LOG.warn("Volume " + v + " is closed, ignore the deletion task for " +
            "block " + invalidBlks[i]);
      }
    }
    if (!errors.isEmpty()) {
      StringBuilder b = new StringBuilder("Failed to delete ")
        .append(errors.size()).append(" (out of ").append(invalidBlks.length)
        .append(") replica(s):");
      for(int i = 0; i < errors.size(); i++) {
        b.append("\n").append(i).append(") ").append(errors.get(i));
      }
      throw new IOException(b.toString());
    }
  }

  void cacheBlock(String bpid, long blockId) {
    FsVolumeImpl volume;
    String blockFileName;
    long length, genstamp;
    Executor volumeExecutor;

    synchronized (getLock(blockId)) {
      ReplicaInfo info = volumeMap.get(bpid, blockId);
      boolean success = false;
      try {
        if (info == null) {
          LOG.warn("Failed to cache block with id " + blockId + ", pool " +
              bpid + ": ReplicaInfo not found.");
          return;
        }
        if (info.getState() != ReplicaState.FINALIZED) {
          LOG.warn("Failed to cache block with id " + blockId + ", pool " +
              bpid + ": replica is not finalized; it is in state " +
              info.getState());
          return;
        }
        try {
          volume = (FsVolumeImpl)info.getVolume();
          if (volume == null) {
            LOG.warn("Failed to cache block with id " + blockId + ", pool " +
                bpid + ": volume not found.");
            return;
          }
        } catch (ClassCastException e) {
          LOG.warn("Failed to cache block with id " + blockId +
              ": volume was not an instance of FsVolumeImpl.");
          return;
        }
        if (volume.isTransientStorage()) {
          LOG.warn("Caching not supported on block with id " + blockId +
              " since the volume is backed by RAM.");
          return;
        }
        success = true;
      } finally {
        if (!success) {
          cacheManager.numBlocksFailedToCache.incrementAndGet();
        }
      }
      blockFileName = info.getBlockFile().getAbsolutePath();
      length = info.getVisibleLength();
      genstamp = info.getGenerationStamp();
      volumeExecutor = volume.getCacheExecutor();
    }
    cacheManager.cacheBlock(blockId, bpid, 
        blockFileName, length, genstamp, volumeExecutor);
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }

  @Override
  public long getReplicaVisibleLength(ExtendedBlock block) throws IOException {
    // TODO consider to add read lock for this? No?
    final Replica replica = getReplicaInfo(block.getBlockPoolId(), 
      block.getBlockId());
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException("replica.getGenerationStamp() < block.getGenerationStamp(), block="
        + block + ", replica=" + replica);
    }
    return replica.getVisibleLength();
  }

  @Override
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock) throws IOException {
    synchronized (getLock(rBlock.getBlock().getBlockId())) {
      return initReplicaRecovery(rBlock.getBlock().getBlockPoolId(), rBlock.getBlock()
        .getLocalBlock(), rBlock.getNewGenerationStamp(), datanode.getDnConf()
        .getXceiverStopTimeout());
    }
  }

  ReplicaRecoveryInfo initReplicaRecovery(String bpid, Block block,
    long recoveryId, long xceiverStopTimeout) throws IOException {
    final ReplicaInfo replica = volumeMap.get(bpid, block.getBlockId());
    LOG.info("initReplicaRecovery: " + block + ", recoveryId=" + recoveryId + ", replica="
      + replica);

    // check replica
    if (replica == null) {
      return null;
    }

    // stop writer if there is any
    if (replica instanceof ReplicaInPipeline) {
      final ReplicaInPipeline rip = (ReplicaInPipeline) replica;
      rip.stopWriter(xceiverStopTimeout);

      // check replica bytes on disk.
      if (rip.getBytesOnDisk() < rip.getVisibleLength()) {
        throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
          + " getBytesOnDisk() < getVisibleLength(), rip=" + rip);
      }

      // check the replica's files
      checkReplicaFiles(rip);
    }

    // check generation stamp
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException("replica.getGenerationStamp() < block.getGenerationStamp(), block="
        + block + ", replica=" + replica);
    }

    // check recovery id
    if (replica.getGenerationStamp() >= recoveryId) {
      throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
        + " replica.getGenerationStamp() >= recoveryId = " + recoveryId + ", block=" + block
        + ", replica=" + replica);
    }

    // check RUR
    final ReplicaUnderRecovery rur;
    if (replica.getState() == ReplicaState.RUR) {
      rur = (ReplicaUnderRecovery) replica;
      if (rur.getRecoveryID() >= recoveryId) {
        throw new RecoveryInProgressException("rur.getRecoveryID() >= recoveryId = " + recoveryId
          + ", block=" + block + ", rur=" + rur);
      }
      final long oldRecoveryID = rur.getRecoveryID();
      rur.setRecoveryID(recoveryId);
      LOG.info("initReplicaRecovery: update recovery id for " + block + " from " + oldRecoveryID
        + " to " + recoveryId);
    } else {
      rur = new ReplicaUnderRecovery(replica, recoveryId);
      synchronized (this) {
        volumeMap.add(bpid, rur);
      }
      LOG.info("initReplicaRecovery: changing replica state for " + block + " from "
        + replica.getState() + " to " + rur.getState());
    }
    return rur.createInfo();
  }

  @Override
  public String updateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId,
    long newBlockId, long newLength) throws IOException {
    synchronized(getLock(oldBlock.getBlockId())) {
      //get replica
      final String bpid = oldBlock.getBlockPoolId();
      final ReplicaInfo replica = volumeMap.get(bpid, oldBlock.getBlockId());
      LOG.info("updateReplica: " + oldBlock
                   + ", recoveryId=" + recoveryId
                   + ", length=" + newLength
                   + ", replica=" + replica);

      //check replica
      if (replica == null) {
        throw new ReplicaNotFoundException(oldBlock);
      }

      //check replica state
      if (replica.getState() != ReplicaState.RUR) {
        throw new IOException("replica.getState() != " + ReplicaState.RUR
            + ", replica=" + replica);
      }

      //check replica's byte on disk
      if (replica.getBytesOnDisk() != oldBlock.getNumBytes()) {
        throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
            + " replica.getBytesOnDisk() != block.getNumBytes(), block="
            + oldBlock + ", replica=" + replica);
      }

      //check replica files before update
      checkReplicaFiles(replica);

      //update replica
      final FinalizedReplica finalized = updateReplicaUnderRecovery(oldBlock
          .getBlockPoolId(), (ReplicaUnderRecovery) replica, recoveryId,
          newBlockId, newLength);

      boolean copyTruncate = newBlockId != oldBlock.getBlockId();
      if(!copyTruncate) {
        assert finalized.getBlockId() == oldBlock.getBlockId()
            && finalized.getGenerationStamp() == recoveryId
            && finalized.getNumBytes() == newLength
            : "Replica information mismatched: oldBlock=" + oldBlock
                + ", recoveryId=" + recoveryId + ", newlength=" + newLength
                + ", newBlockId=" + newBlockId + ", finalized=" + finalized;
      } else {
        assert finalized.getBlockId() == oldBlock.getBlockId()
            && finalized.getGenerationStamp() == oldBlock.getGenerationStamp()
            && finalized.getNumBytes() == oldBlock.getNumBytes()
            : "Finalized and old information mismatched: oldBlock=" + oldBlock
                + ", genStamp=" + oldBlock.getGenerationStamp()
                + ", len=" + oldBlock.getNumBytes()
                + ", finalized=" + finalized;
      }

      //check replica files after update
      checkReplicaFiles(finalized);

      //return storage ID
      // TODO consider to use a non-sync method if this method is synchronized to avoid dead lock
      return getVolume(new ExtendedBlock(bpid, finalized)).getStorageID();
    }
  }

  private FinalizedReplica updateReplicaUnderRecovery(String bpid, ReplicaUnderRecovery rur,
    long recoveryId, long newBlockId, long newlength) throws IOException {
    // check recovery id
    if (rur.getRecoveryID() != recoveryId) {
      throw new IOException("rur.getRecoveryID() != recoveryId = " + recoveryId + ", rur=" + rur);
    }

    boolean copyOnTruncate = newBlockId > 0L && rur.getBlockId() != newBlockId;
    File blockFile;
    File metaFile;
    // bump rur's GS to be recovery id
    if (!copyOnTruncate) {
      bumpReplicaGS(rur, recoveryId);
      blockFile = rur.getBlockFile();
      metaFile = rur.getMetaFile();
    } else {
      File[] copiedReplicaFiles = copyReplicaWithNewBlockIdAndGS(rur, bpid, newBlockId, recoveryId);
      blockFile = copiedReplicaFiles[1];
      metaFile = copiedReplicaFiles[0];
    }

    // update length
    if (rur.getNumBytes() < newlength) {
      throw new IOException("rur.getNumBytes() < newlength = " + newlength + ", rur=" + rur);
    }
    if (rur.getNumBytes() > newlength) {
      truncateBlock(blockFile, metaFile, rur.getNumBytes(), newlength);
      if (!copyOnTruncate) {
        // update RUR with the new length
        rur.setNumBytes(newlength);
      } else {
        // Copying block to a new block with new blockId.
        // Not truncating original block.
        ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(newBlockId, recoveryId,
          rur.getVolume(), blockFile.getParentFile(), newlength);
        newReplicaInfo.setNumBytes(newlength);
        synchronized (this) {
          volumeMap.add(bpid, newReplicaInfo);
        }
        finalizeReplica(bpid, newReplicaInfo);
      }
    }

    // finalize the block
    return finalizeReplica(bpid, rur);
  }

}