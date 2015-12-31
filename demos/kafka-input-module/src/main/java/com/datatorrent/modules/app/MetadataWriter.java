package com.datatorrent.modules.app;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.io.input.AbstractFileSplitter;
import com.datatorrent.lib.io.input.ModuleFileSplitter;

class MetadataWriter extends AbstractFileOutputOperator<AbstractFileSplitter.FileMetadata>
{
  String fileName;

  @SuppressWarnings("unused")
  public MetadataWriter()
  {

  }

  public MetadataWriter(String fileName)
  {
    this.fileName = fileName;
  }

  @Override
  protected String getFileName(AbstractFileSplitter.FileMetadata tuple)
  {
    return fileName;
  }

  @Override
  protected byte[] getBytesForTuple(AbstractFileSplitter.FileMetadata tuple)
  {
    return ((ModuleFileSplitter.ModuleFileMetaData)tuple).toString().getBytes();
  }
}
