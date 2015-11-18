package com.datatorrent.demos.moduleapps;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.io.fs.AbstractFileSplitter;

public class HdfsOutputOperator extends AbstractFileOutputOperator<AbstractFileSplitter.FileMetadata>
{
  @Override
  protected String getFileName(AbstractFileSplitter.FileMetadata tuple)
  {
    return "testResult.txt";
  }

  @Override
  protected byte[] getBytesForTuple(AbstractFileSplitter.FileMetadata tuple)
  {
    return tuple.getFileName().getBytes();
  }
}
