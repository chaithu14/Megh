package com.datatorrent.demos.moduleapps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.module.HDFSInputModule;

@ApplicationAnnotation(name="Kafka2Dedup")
public class Kafka2Parser2Dedup2HdfsApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HDFSInputModule module = dag.addModule("hdfsInputModule", HDFSInputModule.class);
    HdfsOutputOperator output = dag.addOperator("HdfsOUtput", HdfsOutputOperator.class);
    dag.addOperator("FileWriter", output);
    DevNull devNull1 = dag.addOperator("devNull1", new DevNull());
    DevNull devNull2 = dag.addOperator("devNull2", new DevNull());
    dag.addStream("FileMetaData", module.filesMetadataOutput, output.input);
    dag.addStream("devNull_1", module.blocksMetadataOutput, devNull1.data);
    dag.addStream("devNull_2", module.messages, devNull2.data);
  }

}

