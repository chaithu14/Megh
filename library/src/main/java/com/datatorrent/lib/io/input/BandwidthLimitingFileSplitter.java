package com.datatorrent.lib.io.input;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.lib.io.fs.FileSplitterInput;

public class BandwidthLimitingFileSplitter extends FileSplitterInput
{
  private FileBlockMetadata currentBlockMetadata;

  public BandwidthLimitingFileSplitter()
  {
    super();
    super.setScanner(new Scanner());
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    //TODO: setup bandwidth manager
    //TODO: One time copy counters...
  }

  @Override
  public void teardown()
  {
    super.teardown();
    //    bandwidthManager.teardown();
  }

  @Override
  public void emitTuples()
  {
    // TODO emit considering bandwidth
    super.emitTuples();
  }

  @Override
  protected boolean emitBlockMetadata()
  {
    // TODO Auto-generated method stub
    return super.emitBlockMetadata();
  }

  public static class Scanner extends TimeBasedDirectoryScanner
  {
    private String ignoreFilePatternRegularExp;
    private transient Pattern ignoreRegex;

    @Override
    public void setup(OperatorContext context)
    {
      if (ignoreFilePatternRegularExp != null) {
        ignoreRegex = Pattern.compile(this.ignoreFilePatternRegularExp);
      }
      super.setup(context);
    }

    public String getIgnoreFilePatternRegularExp()
    {
      return ignoreFilePatternRegularExp;
    }

    public void setIgnoreFilePatternRegularExp(String ignoreFilePatternRegularExp)
    {
      this.ignoreFilePatternRegularExp = ignoreFilePatternRegularExp;
      this.ignoreRegex = Pattern.compile(ignoreFilePatternRegularExp);
    }

    @Override
    protected boolean acceptFile(String filePathStr)
    {
      boolean accepted = super.acceptFile(filePathStr);
      if (!accepted) {
        return false;
      }
      String fileName = new Path(filePathStr).getName();
      if (ignoreRegex != null) {
        Matcher matcher = ignoreRegex.matcher(fileName);
        // If matched against ignored Regex then do not accept the file.
        if (matcher.matches()) {
          return false;
        }
      }
      return true;
    }

    //override scan complete for one time copy

  }

  public static class ModuleFileMetaData extends BandwidthLimitingFileSplitter.FileMetadata
  {
    private String relativePath;

    public String getRelativePath()
    {
      return relativePath;
    }

    public void setRelativePath(String relativePath)
    {
      this.relativePath = relativePath;
    }

    public String getOutputRelativePath()
    {
      return relativePath;
    }

  }
}
