package com.datatorrent.modules.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.POJOKafkaOutputOperator;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.module.KafkaInputModule;

@ApplicationAnnotation(name = "KafkaOutputBenchmarkApp")
public class KafkaOutputBenchmarkApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    PojoTuplesGeneratorOperator input = dag.addOperator("Generator", new PojoTuplesGeneratorOperator());
    POJOKafkaOutputOperator output = dag.addOperator("KafkaWrite", POJOKafkaOutputOperator.class);
    dag.addStream("Messages", input.outputPort, output.inputPort).setLocality(DAG.Locality.THREAD_LOCAL);
  }
}