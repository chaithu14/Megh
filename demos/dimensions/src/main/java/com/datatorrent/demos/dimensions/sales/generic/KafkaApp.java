package com.datatorrent.demos.dimensions.sales.generic;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.modules.kafkainput.KafkaInputModule;

@ApplicationAnnotation(name="KafkaDemo")
public class KafkaApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaInputModule km = dag.addModule("MessageR", new KafkaInputModule());
    ConsoleOutputOperator output = dag.addOperator("MessageWriter", new ConsoleOutputOperator());
    dag.addStream("Messages", km.output, output.input);
  }

}

