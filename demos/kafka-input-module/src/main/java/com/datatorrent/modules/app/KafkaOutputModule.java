package com.datatorrent.modules.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "KafkaOutputApp")
public class KafkaOutputModule implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomGenerator input = dag.addOperator("Generator", new RandomGenerator());
    KafkaOutputModule output = dag.addModule("KafkaWriter", new KafkaOutputModule());
    dag.addStream("Messages", input.byteoutput, output.input).setLocality(DAG.Locality.THREAD_LOCAL);
    input.setDisableGenerate(false);
    input.setInitialOffset("earliest");
    input.setTuplesBlast(1);
  }
}