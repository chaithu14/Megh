package com.datatorrent;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.io.fs.AbstractFileSplitter.FileMetadata;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.module.HDFSInputModule;

public class HDFSInputModuleAppTest
{

  private String inputDir;
  private String outputDir;
  private StreamingApplication app;

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
    inputDir = testMeta.baseDirectory + File.separator + "input";
    outputDir = testMeta.baseDirectory + File.separator + "output";

    app = new StreamingApplication()
    {
      public void populateDAG(DAG dag, Configuration conf)
      {
        HDFSInputModule module = dag.addModule("hdfsInputModule", HDFSInputModule.class);
        AbstractFileOutputOperator<FileMetadata> outputOperator = new AbstractFileOutputOperator<FileMetadata>()
        {

          @Override
          protected String getFileName(FileMetadata tuple)
          {
            return "testResult.txt";
          }

          @Override
          protected byte[] getBytesForTuple(FileMetadata tuple)
          {
            return tuple.getFileName().getBytes();
          }
        };
        outputOperator.setFilePath(outputDir);

        dag.addOperator("FileWriter", outputOperator);
        DevNull devNull1 = dag.addOperator("devNull1", new DevNull());
        DevNull devNull2 = dag.addOperator("devNull2", new DevNull());
        dag.addStream("FileMetaData", module.filesMetadataOutput, outputOperator.input);
        dag.addStream("devNull_1", module.blocksMetadataOutput, devNull1.data);
        dag.addStream("devNull_2", module.messages, devNull2.data);
      }
    };

  }

  @Test
  public void test() throws Exception
  {

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
    Path outDir = new Path("file://" + new File(outputDir).getAbsolutePath());
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 20000) {
      Thread.sleep(500);
      //          LOG.debug("Waiting for {}", outDir);
    }

    Thread.sleep(10000);
    lc.shutdown();

    Assert.assertTrue("output dir does not exist", fs.exists(outDir));
//    Assert.assertTrue(new File(outputDir).exists());
    System.out.println("Running test.");
  }
}
