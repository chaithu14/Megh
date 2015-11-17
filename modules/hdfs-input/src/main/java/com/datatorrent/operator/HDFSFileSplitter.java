package com.datatorrent.operator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.input.BandwidthLimitingFileSplitter;

public class HDFSFileSplitter extends BandwidthLimitingFileSplitter
{
  private transient FileSystem appFS;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    Scanner scanner = (Scanner)getScanner();
    if (scanner.getIgnoreFilePatternRegularExp() == null) {
      scanner.setIgnoreFilePatternRegularExp(".*._COPYING_");
    }
    try {
      appFS = getLocalFS();
    } catch (IOException e) {
      throw new RuntimeException("Unable to get FileSystem instance.", e);
    }
  }

  private FileSystem getLocalFS() throws IOException
  {
    return FileSystem.newInstance(new Configuration());
  }

  @Override
  protected long getDefaultBlockSize()
  {
    //Really, block size of local HDFS? Input module is not going to write right away, may be we need remote block size only.
    return appFS.getDefaultBlockSize(new Path(context.getValue(DAGContext.APPLICATION_PATH)));
  }

  @Override
  public void teardown()
  {
    super.teardown();
    if (appFS != null) {
      try {
        appFS.close();
      } catch (IOException e) {
        throw new RuntimeException("Unable to close application file system.", e);
      }
    }
  }

  public static class HDFSScanner extends Scanner
  {
    @Override
    protected boolean acceptFile(String filePathStr)
    {
      boolean accepted = super.acceptFile(filePathStr);
      if (containsUnsupportedCharacters(filePathStr)) {
        return false;
      }
      return accepted;
    }

    private boolean containsUnsupportedCharacters(String filePathStr)
    {
      return new Path(filePathStr).toUri().getPath().contains(":");
    }
  }
}
