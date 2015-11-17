package com.datatorrent.module;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.common.metric.MetricsAggregator;
import com.datatorrent.common.metric.SingleMetricAggregator;
import com.datatorrent.common.metric.sum.LongSumAggregator;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.block.AbstractBlockReader.ReaderRecord;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.lib.io.fs.AbstractFileSplitter.FileMetadata;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.operator.HDFSBlockReader;
import com.datatorrent.operator.HDFSFileSplitter;

public class Module implements com.datatorrent.api.Module
{
  @NotNull
  @Size(min = 1)
  private String files;
  private String filePatternRegularExp;
  private long bandwidth;
  @Min(0)
  private long scanIntervalMillis;
  private boolean recursive;
  private Long blockSize;

  public final transient ProxyOutputPort<FileMetadata> filesMetadataOutput = new ProxyOutputPort();
  public final transient ProxyOutputPort<FileBlockMetadata> blocksMetadataOutput = new ProxyOutputPort();
  public final transient ProxyOutputPort<ReaderRecord<Slice>> messages = new ProxyOutputPort();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HDFSFileSplitter fileSplitter = dag.addOperator("FileSplitter", new HDFSFileSplitter());
    fileSplitter.setBlockSize(blockSize);
    fileSplitter.getScanner().setFiles(files);
    fileSplitter.getScanner().setScanIntervalMillis(scanIntervalMillis);
    fileSplitter.getScanner().setRecursive(recursive);
    fileSplitter.getScanner().setFilePatternRegularExp(filePatternRegularExp);
    fileSplitter.getBandwidthManager().setBandwidth(bandwidth);
    //TODO: set onetime copy

    HDFSBlockReader blockReader = dag.addOperator("BlockReader", new HDFSBlockReader());
    blockReader.setUri(files);

    MetricsAggregator blockReaderMetrics = new MetricsAggregator();
    blockReaderMetrics.addAggregators("bytesReadPerSec", new SingleMetricAggregator[] { new LongSumAggregator() });
    dag.setAttribute(blockReader, Context.OperatorContext.METRICS_AGGREGATOR, blockReaderMetrics);
    dag.setAttribute(blockReader, Context.OperatorContext.COUNTERS_AGGREGATOR,
        new BasicCounters.LongAggregator<MutableLong>());
    //TODO: set partitilaron count on block readers

    filesMetadataOutput.set(fileSplitter.filesMetadataOutput);
    blocksMetadataOutput.set(blockReader.blocksMetadataOutput);
    messages.set(blockReader.messages);

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);
  }

  /**
   * A comma separated list of directories to scan. If the path is not fully
   * qualified the default file system is used. A fully qualified path can be
   * provided to scan directories in other filesystems.
   *
   * @param files files
   */
  public void setFiles(String files)
  {
    this.files = files;
  }

  /**
   * Gets the files to be scanned.
   *
   * @return files to be scanned.
   */
  public String getFiles()
  {
    return files;
  }

  /**
   * Gets the regular expression for file names to split.
   *
   * @return regular expression
   */
  public String getFilePatternRegularExp()
  {
    return filePatternRegularExp;
  }

  /**
   * Only files with names matching the given java regular expression are split.
   *
   * @param filePatternRegexp regular expression
   */
  public void setFilePatternRegularExp(String filePatternRegexp)
  {
    this.filePatternRegularExp = filePatternRegexp;
  }

  public long getBandwidth()
  {
    return bandwidth;
  }

  public void setBandwidth(long bandwidth)
  {
    this.bandwidth = bandwidth;
  }

  public long getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  public void setScanIntervalMillis(long scanIntervalMillis)
  {
    this.scanIntervalMillis = scanIntervalMillis;
  }

  public boolean isRecursive()
  {
    return recursive;
  }

  public void setRecursive(boolean recursive)
  {
    this.recursive = recursive;
  }

  public Long getBlockSize()
  {
    return blockSize;
  }

  public void setBlockSize(Long blockSize)
  {
    this.blockSize = blockSize;
  }
}
