package com.datatorrent.modules.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.POJOKafkaOutputOperator;

@ApplicationAnnotation(name = "KafkaOutputApp")
public class KafkaOutputModule implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomGenerator input = dag.addOperator("Generator", new RandomGenerator());
    POJOKafkaOutputOperator output = dag.addOperator("KafkaWriter", new POJOKafkaOutputOperator());
    dag.addStream("Messages", input.byteoutput, output.inputPort);
    input.setDisableGenerate(false);
    input.setInitialOffset("earliest");
  }
}