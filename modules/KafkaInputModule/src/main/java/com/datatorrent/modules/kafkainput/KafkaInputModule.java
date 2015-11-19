package com.datatorrent.modules.kafkainput;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.contrib.kafka.KafkaSinglePortByteArrayInputOperator;

public class KafkaInputModule implements Module
{
  public transient ProxyOutputPort<byte[]> output = new Module.ProxyOutputPort<byte[]>();

  @NotNull
  private String topic;

  @NotNull
  private String zookeeper;

  private String offsetFilePath;

  private String initialOffset;

  @Min(1)
  private int parallelReads = 1;

  @Min(1)
  private int maxTuplesPerWindow = Integer.MAX_VALUE;

  private String strategy = "one_to_many";

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public void setZookeeper(String zookeeper)
  {
    this.zookeeper = zookeeper;
  }

  public void setOffsetFilePath(String offsetFilePath) {
    this.offsetFilePath = offsetFilePath;
  }

  public void setInitialOffset(String initialOffset) {
    this.initialOffset = initialOffset;
  }

  public void setParallelReads(int parallelReads) {
    this.parallelReads = parallelReads;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow) { this.maxTuplesPerWindow = maxTuplesPerWindow; }

  public void setStrategy(String strategy) { this.strategy = strategy; }

  /**
   * Kafka Input module dag consists of single Kafka input operator
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortByteArrayInputOperator input = dag.addOperator("MessageReader", new KafkaSinglePortByteArrayInputOperator());

    // Set the offset manager if the offsetFilePath is not null
    if(offsetFilePath != null) {
      HdfsOffsetManager offsetManager = new HdfsOffsetManager();
      offsetManager.setDelimiter(",");
      offsetManager.setOffsetPath(offsetFilePath);
      offsetManager.setTopicName(topic);
      input.setOffsetManager(offsetManager);
    }

    input.setTopic(topic);
    input.setZookeeper(zookeeper);

    if(initialOffset != null) {
      input.setInitialOffset(initialOffset);
    }

    // Disabling dynamic partition
    input.setRepartitionInterval(-1);
    input.setMaxTuplesPerWindow(maxTuplesPerWindow);

    input.setInitialPartitionCount(parallelReads);

    input.setStrategy(strategy);

    output.set(input.outputPort);
  }
}