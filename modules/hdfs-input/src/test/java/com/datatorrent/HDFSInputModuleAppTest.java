package com.datatorrent;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.io.fs.AbstractFileSplitter.FileMetadata;
import com.datatorrent.module.HDFSInputModule;

public class HDFSInputModuleAppTest
{

  private String inputDir;

  public static class TestMeta extends TestWatcher
  {
    public String baseDirectory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Before
  public void setup() throws Exception
  {
    StreamingApplication app = new StreamingApplication()
    {
      public void populateDAG(DAG dag, Configuration conf)
      {
        HDFSInputModule module = dag.addModule("hdfsInputModule", HDFSInputModule.class);
        AbstractFileOutputOperator<FileMetadata> outputOperator = new AbstractFileOutputOperator<FileMetadata>()
        {

          @Override
          protected String getFileName(FileMetadata tuple)
          {
            return "TestResult.txt";
          }

          @Override
          protected byte[] getBytesForTuple(FileMetadata tuple)
          {
            return tuple.getFileName().getBytes();
          }
        };
        dag.addOperator("FileWriter", outputOperator);
        dag.addStream("FileMetaData", module.filesMetadataOutput, outputOperator.input);
      }
    };

    inputDir = testMeta.baseDirectory + File.separator + "input";
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.module.hdfsInputModule.prop.files", inputDir);
    conf.set("dt.module.hdfsInputModule.prop.blockSize", "10");

    FileUtils.writeStringToFile(new File(inputDir + File.separator + "file1.txt"), "File one data");
    FileUtils.writeStringToFile(new File(inputDir + File.separator + "file2.txt"), "File two data");

    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();

    long now = System.currentTimeMillis();

    Thread.sleep(10000);
    lc.shutdown();
  }

  @Test
  public void test()
  {
    System.out.println("Running test.");
  }
}
