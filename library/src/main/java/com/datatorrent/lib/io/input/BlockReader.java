package com.datatorrent.lib.io.input;

import java.net.URI;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.block.ModuleBlockMetadata;
import com.datatorrent.netlet.util.Slice;

@StatsListener.DataQueueSize
/**
 * <p>BlockReader class.</p>
 *
 * @since 3.2.0
 */
public class BlockReader extends FSSliceReader
{
  protected int maxRetries;
  protected Queue<FailedBlock> failedQueue;

  @AutoMetric
  private long bytesReadPerSec;

  private long bytesRead;
  private double windowTimeSec;

  @OutputPortFieldAnnotation(optional = true, error = true)
  public final transient DefaultOutputPort<ModuleBlockMetadata> error = new DefaultOutputPort<ModuleBlockMetadata>();

  public BlockReader()
  {
    super();
    maxRetries = 0;
    failedQueue = Lists.newLinkedList();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)
        * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void handleIdleTime()
  {
    if (!failedQueue.isEmpty()) {
      FailedBlock failedBlock = failedQueue.poll();
      failedBlock.retries++;
      processBlockMetadata(failedBlock);
    } else {
      super.handleIdleTime();
    }
  }

  @Override
  protected void processBlockMetadata(BlockMetadata.FileBlockMetadata block)
  {
    ModuleBlockMetadata moduleBlock = (ModuleBlockMetadata)block;
    if (maxRetries == 0) {
      super.processBlockMetadata(moduleBlock);
    } else {
      try {
        super.processBlockMetadata(moduleBlock);
      } catch (Throwable t) {
        if (moduleBlock instanceof FailedBlock) {
          //A failed block was being processed
          FailedBlock failedBlock = (FailedBlock)moduleBlock;
          LOG.debug("attempt {} to process block {} failed", failedBlock.retries, failedBlock.block.getBlockId());
          if (failedBlock.retries < maxRetries) {
            failedQueue.add(failedBlock);
          } else if (error.isConnected()) {
            error.emit(failedBlock.block);
          }
        } else {
          LOG.debug("failed to process block {}", block.getBlockId());
          failedQueue.add(new FailedBlock(moduleBlock));
        }
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    bytesRead = 0;
    bytesReadPerSec = 0;
  }

  @Override
  public void endWindow()
  {
    bytesReadPerSec = (long)(bytesRead / windowTimeSec);
    super.endWindow();
  }

  @Override
  protected Slice convertToRecord(byte[] bytes)
  {
    bytesRead += bytes.length;
    return super.convertToRecord(bytes);
  }

  private static final Logger LOG = LoggerFactory.getLogger(BlockReader.class);

  protected static class FailedBlock extends ModuleBlockMetadata
  {
    protected int retries;
    protected final ModuleBlockMetadata block;

    @SuppressWarnings("unused")
    private FailedBlock()
    {
      block = null;
    }

    FailedBlock(ModuleBlockMetadata block)
    {
      this.block = block;
    }

    @Override
    public long getBlockId()
    {
      return block.getBlockId();
    }
  }

  /**
   * Sets the max number of retries.
   *
   * @param maxRetries
   *          maximum number of retries
   */
  public void setMaxRetries(int maxRetries)
  {
    this.maxRetries = maxRetries;
  }

  /**
   * @return the max number of retries.
   */
  public int getMaxRetries()
  {
    return this.maxRetries;
  }

  int getOperatorId()
  {
    return operatorId;
  }

  Set<Integer> getPartitionKeys()
  {
    return this.partitionKeys;
  }

  void setPartitionKeys(Set<Integer> partitionKeys)
  {
    this.partitionKeys = partitionKeys;
  }

  int getPartitionMask()
  {
    return this.partitionMask;
  }

  void setPartitionMask(int partitionMask)
  {
    this.partitionMask = partitionMask;
  }

  BasicCounters<MutableLong> getCounters()
  {
    return this.counters;
  }
}
