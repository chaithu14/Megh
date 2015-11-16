package com.datatorrent.modules.kafkainput;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

public class KafkaInputModule implements Module
{
  public transient ProxyOutputPort<MutablePair<String,byte[]>> kOutputPort = new Module.ProxyOutputPort<MutablePair<String,byte[]>>();

  @NotNull
  private String topic;

  @NotNull
  private String zookeeper;

  private String offsetPathName;

  private String initialOffset;

  private int parallelReads = 1;

  public void setTopic(String _topic)
  {
    this.topic = _topic;
  }

  public void setZookeeper(String _zookeeper)
  {
    this.zookeeper = _zookeeper;
  }

  public void setOffsetPathName(String offsetPathName) {
    this.offsetPathName = offsetPathName;
  }

  public void setInitialOffset(String initialOffset) {
    this.initialOffset = initialOffset;
  }

  public void setParallelReads(int parallelReads) {
    this.parallelReads = parallelReads;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HdfsOffsetManager offsetManager = new HdfsOffsetManager();
    offsetManager.setDelimiter(",");

    if(offsetPathName != null) {
      offsetManager.setOffsetPath(offsetPathName);
    }
    offsetManager.setTopicName(topic);

    KafkaByteArrayInputOperator input = dag.addOperator("MessageReader", new KafkaByteArrayInputOperator());
    if(offsetPathName != null) {
      input.setOffsetManager(offsetManager);
    }

    input.setTopic(topic);
    input.setZookeeper(zookeeper);

    if(initialOffset != null) {
      input.setInitialOffset(initialOffset);
    }

    input.setInitialPartitionCount(parallelReads);

    kOutputPort.set(input.outputPort);
  }
}