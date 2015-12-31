package com.datatorrent.modules.app;

import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.netlet.util.Slice;

public class FileWriter extends AbstractFileOutputOperator<AbstractBlockReader.ReaderRecord<Slice>>
{
  String fileName;

  @SuppressWarnings("unused")
  public FileWriter()
  {
  }

  public FileWriter(String fileName)
  {
    this.fileName = fileName;
  }

  @Override
  protected String getFileName(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    return fileName;
  }

  @Override
  protected byte[] getBytesForTuple(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    return tuple.getRecord().buffer;
  }

}
