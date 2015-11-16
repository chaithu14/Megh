package com.datatorrent.operator;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;

import com.datatorrent.lib.io.input.BlockReader;

public class HDFSBlockReader extends BlockReader
{

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    return FileSystem.newInstance(URI.create(uri), configuration);
  }
}
