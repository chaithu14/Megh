package com.datatorrent.contrib.hdht;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.netlet.util.Slice;

public class HDHTStore implements HDHT.Writer, HDHT.Reader
{
  private final transient HashMap<Long, HDHTReader.BucketMeta> metaCache = Maps.newHashMap();
  private long currentWindowId;
  private transient long lastFlushWindowId;
  private final transient HashMap<Long, Bucket> buckets = Maps.newHashMap();
  @VisibleForTesting
  public transient ExecutorService writeExecutor;
  private volatile transient Throwable writerError;

  private int maxFileSize = 128 * 1024 * 1024; // 128m
  private int maxWalFileSize = 64 * 1024 * 1024;
  private int flushSize = 1000000;
  private int flushIntervalCount = 120;

  private final HashMap<Long, WalMeta> walMeta = Maps.newHashMap();
  private transient Context.OperatorContext context;

  /**
   * Size limit for data files. Files are rolled once the limit has been exceeded. The final size of a file can be
   * larger than the limit by the size of the last/single entry written to it.
   *
   * @return The size limit for data files.
   */
  public int getMaxFileSize()
  {
    return maxFileSize;
  }

  public void setMaxFileSize(int maxFileSize)
  {
    this.maxFileSize = maxFileSize;
  }

  /**
   * Size limit for WAL files. Files are rolled once the limit has been exceeded. The final size of a file can be larger
   * than the limit, as files are rolled at end of the operator window.
   *
   * @return The size limit for WAL files.
   */
  public int getMaxWalFileSize()
  {
    return maxWalFileSize;
  }

  public void setMaxWalFileSize(int maxWalFileSize)
  {
    this.maxWalFileSize = maxWalFileSize;
  }

  /**
   * The number of changes collected in memory before flushing to persistent storage.
   *
   * @return The number of changes collected in memory before flushing to persistent storage.
   */
  public int getFlushSize()
  {
    return flushSize;
  }

  public void setFlushSize(int flushSize)
  {
    this.flushSize = flushSize;
  }

  /**
   * Cached writes are flushed to persistent storage periodically. The interval is specified as count of windows and
   * establishes the maximum latency for changes to be written while below the {@link #flushSize} threshold.
   *
   * @return The flush interval count.
   */
  @Min(value = 1)
  public int getFlushIntervalCount()
  {
    return flushIntervalCount;
  }

  public void setFlushIntervalCount(int flushIntervalCount)
  {
    this.flushIntervalCount = flushIntervalCount;
  }

  /**
   * Write data to size based rolling files
   *
   * @param bucket
   * @param bucketMeta
   * @param data
   * @throws java.io.IOException
   */
  private void writeFile(Bucket bucket, HDHTReader.BucketMeta bucketMeta, TreeMap<Slice, byte[]> data) throws IOException
  {
    HDHTWriter.BucketIOStats ioStats = getOrCretaStats(bucket.bucketKey);
    long startTime = System.currentTimeMillis();

    HDHTFileAccess.HDSFileWriter fw = null;
    HDHTReader.BucketFileMeta fileMeta = null;
    int keysWritten = 0;
    for (Map.Entry<Slice, byte[]> dataEntry : data.entrySet()) {
      if (fw == null) {
        // next file
        fileMeta = bucketMeta.addFile(bucket.bucketKey, dataEntry.getKey());
        LOG.debug("writing data file {} {}", bucket.bucketKey, fileMeta.name);
        fw = this.store.getWriter(bucket.bucketKey, fileMeta.name + ".tmp");
        keysWritten = 0;
      }

      if (dataEntry.getValue() == HDHT.WALReader.DELETED) {
        continue;
      }

      fw.append(dataEntry.getKey().toByteArray(), dataEntry.getValue());
      keysWritten++;
      if (fw.getBytesWritten() > this.maxFileSize) {
        ioStats.dataFilesWritten++;
        ioStats.filesWroteInCurrentWriteCycle++;
        // roll file
        fw.close();
        ioStats.dataBytesWritten += fw.getBytesWritten();
        this.store.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
        LOG.debug("created data file {} {} with {} entries", bucket.bucketKey, fileMeta.name, keysWritten);
        fw = null;
        keysWritten = 0;
      }
    }

    if (fw != null) {
      ioStats.dataFilesWritten++;
      ioStats.filesWroteInCurrentWriteCycle++;
      fw.close();
      ioStats.dataBytesWritten += fw.getBytesWritten();
      this.store.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
      LOG.debug("created data file {} {} with {} entries", bucket.bucketKey, fileMeta.name, keysWritten);
    }

    ioStats.dataWriteTime += System.currentTimeMillis() - startTime;
  }

  private Bucket getBucket(long bucketKey) throws IOException
  {
    Bucket bucket = this.buckets.get(bucketKey);
    if (bucket == null) {
      LOG.debug("Opening bucket {}", bucketKey);
      bucket = new Bucket();
      bucket.bucketKey = bucketKey;
      this.buckets.put(bucketKey, bucket);

      HDHTReader.BucketMeta bmeta = getMeta(bucketKey);
      WalMeta wmeta = getWalMeta(bucketKey);
      bucket.wal = new HDHTWalManager(this.store, bucketKey, wmeta.cpWalPosition);
      bucket.wal.setMaxWalFileSize(maxWalFileSize);
      HDHTWriter.BucketIOStats ioStats = getOrCretaStats(bucketKey);
      if (ioStats != null) {
        bucket.wal.restoreStats(ioStats);
      }

      LOG.debug("walStart {} walEnd {} windowId {} committedWid {} currentWid {}",
        bmeta.recoveryStartWalPosition, wmeta.cpWalPosition, wmeta.windowId, bmeta.committedWid, currentWindowId);

      // bmeta.componentLSN is data which is committed to disks.
      // wmeta.windowId windowId till which data is available in WAL.
      if (bmeta.committedWid < wmeta.windowId && wmeta.windowId != 0) {
        LOG.debug("Recovery for bucket {}", bucketKey);
        // Add tuples from recovery start till recovery end.
        bucket.wal.runRecovery(bucket.committedWriteCache, bmeta.recoveryStartWalPosition, wmeta.cpWalPosition);
        bucket.walPositions.put(wmeta.windowId, wmeta.cpWalPosition);
      }
    }
    return bucket;
  }

  /**
   * Lookup in write cache (data not flushed/committed to files).
   *
   * @param bucketKey
   * @param key
   * @return uncommitted.
   */
  @Override
  public byte[] getUncommitted(long bucketKey, Slice key)
  {
    Bucket bucket = this.buckets.get(bucketKey);
    if (bucket != null) {
      byte[] v = bucket.writeCache.get(key);
      if (v != null) {
        return v != HDHT.WALReader.DELETED ? v : null;
      }
      for (Map.Entry<Long, HashMap<Slice, byte[]>> entry : bucket.checkpointedWriteCache.entrySet()) {
        byte[] v2 = entry.getValue().get(key);
        // find most recent entry
        if (v2 != null) {
          v = v2;
        }
      }
      if (v != null) {
        return v != HDHT.WALReader.DELETED ? v : null;
      }
      v = bucket.committedWriteCache.get(key);
      if (v != null) {
        return v != HDHT.WALReader.DELETED ? v : null;
      }
      v = bucket.frozenWriteCache.get(key);
      return v != null && v != HDHT.WALReader.DELETED ? v : null;
    }
    return null;
  }

  @Override
  public void put(long bucketKey, Slice key, byte[] value) throws IOException
  {
    Bucket bucket = getBucket(bucketKey);
    bucket.wal.append(key, value);
    bucket.writeCache.put(key, value);
  }

  public void delete(long bucketKey, Slice key) throws IOException
  {
    put(bucketKey, key, HDHT.WALReader.DELETED);
  }

  /**
   * Flush changes from write cache to disk. New data files will be written and meta data replaced atomically. The flush
   * frequency determines availability of changes to external readers.
   *
   * @throws IOException
   */
  private void writeDataFiles(Bucket bucket) throws IOException
  {
    HDHTWriter.BucketIOStats ioStats = getOrCretaStats(bucket.bucketKey);
    LOG.debug("Writing data files in bucket {}", bucket.bucketKey);
    // copy meta data on write
    HDHTReader.BucketMeta bucketMetaCopy = kryo.copy(getMeta(bucket.bucketKey));

    // bucket keys by file
    TreeMap<Slice, HDHTReader.BucketFileMeta> bucketSeqStarts = bucketMetaCopy.files;
    Map<HDHTReader.BucketFileMeta, Map<Slice, byte[]>> modifiedFiles = Maps.newHashMap();

    for (Map.Entry<Slice, byte[]> entry : bucket.frozenWriteCache.entrySet()) {
      // find file for key
      Map.Entry<Slice, HDHTReader.BucketFileMeta> floorEntry = bucketSeqStarts.floorEntry(entry.getKey());
      HDHTReader.BucketFileMeta floorFile;
      if (floorEntry != null) {
        floorFile = floorEntry.getValue();
      } else {
        floorEntry = bucketSeqStarts.firstEntry();
        if (floorEntry == null || floorEntry.getValue().name != null) {
          // no existing file or file with higher key
          floorFile = new HDHTReader.BucketFileMeta();
        } else {
          // placeholder for new keys, move start key
          floorFile = floorEntry.getValue();
          bucketSeqStarts.remove(floorEntry.getKey());
        }
        floorFile.startKey = entry.getKey();
        if (floorFile.startKey.length != floorFile.startKey.buffer.length) {
          // normalize key for serialization
          floorFile.startKey = new Slice(floorFile.startKey.toByteArray());
        }
        bucketSeqStarts.put(floorFile.startKey, floorFile);
      }

      Map<Slice, byte[]> fileUpdates = modifiedFiles.get(floorFile);
      if (fileUpdates == null) {
        modifiedFiles.put(floorFile, fileUpdates = Maps.newHashMap());
      }
      fileUpdates.put(entry.getKey(), entry.getValue());
    }

    HashSet<String> filesToDelete = Sets.newHashSet();

    // write modified files
    for (Map.Entry<HDHTReader.BucketFileMeta, Map<Slice, byte[]>> fileEntry : modifiedFiles.entrySet()) {
      HDHTReader.BucketFileMeta fileMeta = fileEntry.getKey();
      TreeMap<Slice, byte[]> fileData = new TreeMap<Slice, byte[]>(getKeyComparator());

      if (fileMeta.name != null) {
        // load existing file
        long start = System.currentTimeMillis();
        HDHTFileAccess.HDSFileReader reader = store.getReader(bucket.bucketKey, fileMeta.name);
        reader.readFully(fileData);
        ioStats.dataBytesRead += store.getFileSize(bucket.bucketKey, fileMeta.name);
        ioStats.dataReadTime += System.currentTimeMillis() - start;
        /* these keys are re-written */
        ioStats.dataKeysRewritten += fileData.size();
        ioStats.filesReadInCurrentWriteCycle++;
        ioStats.dataFilesRead++;
        reader.close();
        filesToDelete.add(fileMeta.name);
      }

      // apply updates
      fileData.putAll(fileEntry.getValue());
      // new file
      writeFile(bucket, bucketMetaCopy, fileData);
    }

    LOG.debug("Files written {} files read {}", ioStats.filesWroteInCurrentWriteCycle, ioStats.filesReadInCurrentWriteCycle);
    // flush meta data for new files
    try {
      LOG.debug("Writing {} with {} file entries", FNAME_META, bucketMetaCopy.files.size());
      OutputStream os = store.getOutputStream(bucket.bucketKey, FNAME_META + ".new");
      Output output = new Output(os);
      bucketMetaCopy.committedWid = bucket.committedLSN;
      bucketMetaCopy.recoveryStartWalPosition = bucket.recoveryStartWalPosition;
      kryo.writeClassAndObject(output, bucketMetaCopy);
      output.close();
      os.close();
      store.rename(bucket.bucketKey, FNAME_META + ".new", FNAME_META);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write bucket meta data " + bucket.bucketKey, e);
    }

    // clear pending changes
    ioStats.dataKeysWritten += bucket.frozenWriteCache.size();
    bucket.frozenWriteCache.clear();
    // switch to new version
    //this.metaCache.put(bucket.bucketKey, bucketMetaCopy);

    // delete old files
    for (String fileName : filesToDelete) {
      store.delete(bucket.bucketKey, fileName);
    }
    invalidateReader(bucket.bucketKey, filesToDelete);

    // cleanup WAL files which are not needed anymore.
    bucket.wal.cleanup(bucketMetaCopy.recoveryStartWalPosition.fileId);

    ioStats.filesReadInCurrentWriteCycle = 0;
    ioStats.filesWroteInCurrentWriteCycle = 0;
  }

  public void setup(Context.OperatorContext context)
  {
    this.store.init();
    writeExecutor = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory(this.getClass().getSimpleName() + "-Writer"));
    this.context = context;
  }

  public void teardown()
  {
    for (HDHTReader.BucketReader bucket : this.readerBuckets.values()) {
      IOUtils.closeQuietly(bucket);
    }
    IOUtils.closeQuietly(store);
    for (Bucket bucket : this.buckets.values()) {
      IOUtils.closeQuietly(bucket.wal);
    }
    writeExecutor.shutdown();
  }

  public void endWindow()
  {
    for (final Bucket bucket : this.buckets.values()) {
      try {
        if (bucket.wal != null) {
          bucket.wal.endWindow(currentWindowId);
          WalMeta walMeta = getWalMeta(bucket.bucketKey);
          walMeta.cpWalPosition = bucket.wal.getCurrentPosition();
          walMeta.windowId = currentWindowId;
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to flush WAL", e);
      }
    }

    // propagate writer exceptions
    if (writerError != null) {
      throw new RuntimeException("Error while flushing write cache.", this.writerError);
    }

    if (context != null) {
      updateStats();
      context.setCounters(bucketStats);
    }
  }

  private WalMeta getWalMeta(long bucketKey)
  {
    WalMeta meta = walMeta.get(bucketKey);
    if (meta == null) {
      meta = new WalMeta();
      walMeta.put(bucketKey, meta);
    }
    return meta;
  }

  public void checkpointed(long windowId)
  {
    for (final Bucket bucket : this.buckets.values()) {
      if (!bucket.writeCache.isEmpty()) {
        bucket.checkpointedWriteCache.put(windowId, bucket.writeCache);
        bucket.walPositions.put(windowId, new HDHTWalManager.WalPosition(
          bucket.wal.getWalFileId(),
          bucket.wal.getWalSize()
        ));
        bucket.writeCache = Maps.newHashMap();
      }
    }
  }

  /**
   * Get meta data from cache or load it on first access
   *
   * @param bucketKey
   * @return The bucket meta.
   */
  private HDHTReader.BucketMeta getMeta(long bucketKey)
  {
    HDHTReader.BucketMeta bm = metaCache.get(bucketKey);
    if (bm == null) {
      bm = loadBucketMeta(bucketKey);
      metaCache.put(bucketKey, bm);
    }
    return bm;
  }

  public void committed(long committedWindowId)
  {
    for (final Bucket bucket : this.buckets.values()) {
      for (Iterator<Map.Entry<Long, HashMap<Slice, byte[]>>> cpIter = bucket.checkpointedWriteCache.entrySet().iterator(); cpIter.hasNext(); ) {
        Map.Entry<Long, HashMap<Slice, byte[]>> checkpointEntry = cpIter.next();
        if (checkpointEntry.getKey() <= committedWindowId) {
          bucket.committedWriteCache.putAll(checkpointEntry.getValue());
          cpIter.remove();
        }
      }

      for (Iterator<Map.Entry<Long, HDHTWalManager.WalPosition>> wpIter = bucket.walPositions.entrySet().iterator(); wpIter.hasNext(); ) {
        Map.Entry<Long, HDHTWalManager.WalPosition> entry = wpIter.next();
        if (entry.getKey() <= committedWindowId) {
          bucket.recoveryStartWalPosition = entry.getValue();
          wpIter.remove();
        }
      }

      if ((bucket.committedWriteCache.size() > this.flushSize || currentWindowId - lastFlushWindowId > flushIntervalCount) && !bucket.committedWriteCache.isEmpty()) {
        // ensure previous flush completed
        if (bucket.frozenWriteCache.isEmpty()) {
          bucket.frozenWriteCache = bucket.committedWriteCache;
          bucket.committedWriteCache = Maps.newHashMap();

          bucket.committedLSN = committedWindowId;

          LOG.debug("Flushing data for bucket {} committedWid {} recoveryStartWalPosition {}", bucket.bucketKey, bucket.committedLSN, bucket.recoveryStartWalPosition);
          Runnable flushRunnable = new Runnable()
          {
            @Override
            public void run()
            {
              try {
                writeDataFiles(bucket);
              } catch (Throwable e) {
                LOG.debug("Write error: {}", e.getMessage());
                writerError = e;
              }
            }
          };
          this.writeExecutor.execute(flushRunnable);
          lastFlushWindowId = committedWindowId;
        }
      }
    }

    // propagate writer exceptions
    if (writerError != null) {
      throw new RuntimeException("Error while flushing write cache.", this.writerError);
    }
  }

  private static class Bucket
  {
    private long bucketKey;
    // keys that were modified and written to WAL, but not yet persisted, by checkpoint
    private HashMap<Slice, byte[]> writeCache = Maps.newHashMap();
    private final LinkedHashMap<Long, HashMap<Slice, byte[]>> checkpointedWriteCache = Maps.newLinkedHashMap();
    public HashMap<Long, HDHTWalManager.WalPosition> walPositions = Maps.newLinkedHashMap();
    private HashMap<Slice, byte[]> committedWriteCache = Maps.newHashMap();
    // keys that are being flushed to data files
    private HashMap<Slice, byte[]> frozenWriteCache = Maps.newHashMap();
    private HDHTWalManager wal;
    private long committedLSN;
    public HDHTWalManager.WalPosition recoveryStartWalPosition;
  }

  @VisibleForTesting
  protected void forceWal() throws IOException
  {
    for (Bucket bucket : buckets.values()) {
      bucket.wal.close();
    }
  }

  @VisibleForTesting
  protected int unflushedDataSize(long bucketKey) throws IOException
  {
    Bucket b = getBucket(bucketKey);
    return b.writeCache.size();
  }

  @VisibleForTesting
  protected int committedDataSize(long bucketKey) throws IOException
  {
    Bucket b = getBucket(bucketKey);
    return b.committedWriteCache.size();
  }

  /* Holds current file Id for WAL and current recoveryEndWalOffset for WAL */
  private static class WalMeta
  {
    /* The current WAL file and recoveryEndWalOffset */
    // Window Id which is written to the WAL.
    public long windowId;

    // Checkpointed WAL position.
    HDHTWalManager.WalPosition cpWalPosition;
  }

  private void updateStats()
  {
    for (Bucket bucket : buckets.values()) {
      HDHTWriter.BucketIOStats ioStats = getOrCretaStats(bucket.bucketKey);
      /* fill in stats for WAL */
      HDHTWalManager.WalStats walStats = bucket.wal.getCounters();
      ioStats.walBytesWritten = walStats.totalBytes;
      ioStats.walFlushCount = walStats.flushCounts;
      ioStats.walFlushTime = walStats.flushDuration;
      ioStats.walKeysWritten = walStats.totalKeys;
      ioStats.dataInWriteCache = bucket.writeCache.size();
      ioStats.dataInFrozenCache = bucket.frozenWriteCache.size();
    }
  }

  /* A map holding stats for each bucket written by this partition */
  private final HashMap<Long, HDHTWriter.BucketIOStats> bucketStats = Maps.newHashMap();

  private HDHTWriter.BucketIOStats getOrCretaStats(long bucketKey)
  {
    HDHTWriter.BucketIOStats ioStats = bucketStats.get(bucketKey);
    if (ioStats == null) {
      ioStats = new HDHTWriter.BucketIOStats();
      bucketStats.put(bucketKey, ioStats);
    }
    return ioStats;
  }

  public static final String FNAME_META = "_META";

  private static final Logger LOG = LoggerFactory.getLogger(HDHTStore.class);

  protected final transient Kryo kryo = new Kryo();
  @NotNull
  protected Comparator<Slice> keyComparator = new HDHTReader.DefaultKeyComparator();
  @Valid
  @NotNull
  protected HDHTFileAccess store;

  public HDHTReader.BucketMeta loadBucketMeta(long bucketKey)
  {
    HDHTReader.BucketMeta bucketMeta = null;
    try {
      InputStream is = store.getInputStream(bucketKey, FNAME_META);
      bucketMeta = (HDHTReader.BucketMeta)kryo.readClassAndObject(new Input(is));
      is.close();
    } catch (IOException e) {
      bucketMeta = new HDHTReader.BucketMeta(keyComparator);
    }
    return bucketMeta;
  }

  private final transient Map<Long, HDHTReader.BucketReader> readerBuckets = Maps.newHashMap();

  /**
   * Compare keys for sequencing as secondary level of organization within buckets.
   * In most cases it will be implemented using a time stamp as leading component.
   *
   * @return The key comparator.
   */
  public Comparator<Slice> getKeyComparator()
  {
    return keyComparator;
  }

  public void setKeyComparator(Comparator<Slice> keyComparator)
  {
    this.keyComparator = keyComparator;
  }

  public HDHTFileAccess getFileStore()
  {
    return store;
  }

  public void setFileStore(HDHTFileAccess fileStore)
  {
    this.store = fileStore;
  }

  protected HDHTReader.BucketReader getReader(long bucketKey)
  {
    HDHTReader.BucketReader br = this.readerBuckets.get(bucketKey);
    if (br == null) {
      this.readerBuckets.put(bucketKey, br = new HDHTReader.BucketReader());
    }
    // meta data can be invalidated on write without removing unaffected readers
    if (br.bucketMeta == null) {
      LOG.debug("Reading {} {}", bucketKey, FNAME_META);
      br.bucketMeta = loadBucketMeta(bucketKey);
    }
    return br;
  }

  protected void invalidateReader(long bucketKey, Set<String> fileNames)
  {
    HDHTReader.BucketReader bucket = this.readerBuckets.get(bucketKey);
    if (bucket != null) {
      bucket.bucketMeta = null; // force index reload
      for (String name : fileNames) {
        LOG.debug("Closing reader {}", name);
        IOUtils.closeQuietly(bucket.readers.remove(name));
      }
    }
  }

  private static Slice GET_KEY = new Slice(null, 0, 0);

  @Override
  public synchronized byte[] get(long bucketKey, Slice key) throws IOException
  {
    // this method is synchronized to support asynchronous reads outside operator thread
    for (int i = 0; i < 10; i++) {
      HDHTReader.BucketReader bucket = getReader(bucketKey);
      HDHTReader.BucketMeta bucketMeta = bucket.bucketMeta;
      if (bucketMeta == null) {
        // meta data invalidated
        continue;
      }

      Map.Entry<Slice, HDHTReader.BucketFileMeta> floorEntry = bucket.bucketMeta.files.floorEntry(key);
      if (floorEntry == null) {
        LOG.info("==============================");
        // no file for this key
        return null;
      }

      try {
        HDHTFileAccess.HDSFileReader reader = bucket.readers.get(floorEntry.getValue().name);
        if (reader == null) {
          LOG.debug("Opening file {} {}", bucketKey, floorEntry.getValue().name);
          bucket.readers.put(floorEntry.getValue().name, reader = store.getReader(bucketKey, floorEntry.getValue().name));
        }
        Slice value = new Slice(null, 0, 0);
        if (reader.seek(key)) {
          reader.next(GET_KEY, value);
        }
        if (value.offset == 0) {
          return value.buffer;
        } else {
          // this is inefficient, should return Slice
          return Arrays.copyOfRange(value.buffer, value.offset, value.offset + value.length);
        }
      } catch (IOException e) {
        // check for meta file update
        this.readerBuckets.remove(bucketKey);
        bucket.close();
        bucket = getReader(bucketKey);
        Map.Entry<Slice, HDHTReader.BucketFileMeta> newEntry = bucket.bucketMeta.files.floorEntry(key);
        if (newEntry != null && newEntry.getValue().name.compareTo(floorEntry.getValue().name) == 0) {
          // file still the same - error unrelated to rewrite
          throw e;
        }
        // retry
        LOG.debug("Retry after meta data change bucket {} from {} to {}", bucketKey, floorEntry, newEntry);
        continue;
      }
    }
    return null;
  }
}

