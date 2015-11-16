package com.datatorrent.module;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.lib.io.fs.AbstractFileSplitter.FileMetadata;
import com.datatorrent.operator.HDFSBlockReader;
import com.datatorrent.operator.HDFSFileSplitter;

/**
 * Placeholder class
 *
 */
public class Module implements com.datatorrent.api.Module
{
  ProxyOutputPort<FileMetadata> filesMetadataOutput = new ProxyOutputPort();
  ProxyOutputPort<FileBlockMetadata> blocksMetadataOutput = new ProxyOutputPort();

  @Override
  public void populateDAG(DAG dag, Configuration arg1)
  {
    dag.addOperator("FileSplitter", new HDFSFileSplitter());
    dag.addOperator("BlockReader", new HDFSBlockReader());
  }
}
