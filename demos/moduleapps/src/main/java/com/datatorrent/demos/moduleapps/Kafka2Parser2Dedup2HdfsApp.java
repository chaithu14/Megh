package com.datatorrent.demos.moduleapps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.module.io.fs.HDFSOutputModule;
import com.datatorrent.modules.kafkainput.KafkaInputModule;

@ApplicationAnnotation(name="Kafka2Dedup")
public class Kafka2Parser2Dedup2HdfsApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaInputModule input = dag.addModule("KafkaReader", new KafkaInputModule());
    HDFSOutputModule output = dag.addModule("HDFSWriter", new HDFSOutputModule());
    dag.addStream("Messages", input.kOutputPort, output.input);
  }

}

