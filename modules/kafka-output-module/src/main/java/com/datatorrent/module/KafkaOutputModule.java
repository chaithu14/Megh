/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.module;

import java.util.Properties;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.contrib.kafka.POJOKafkaOutputOperator;

/**
 * Kafka Output Module will write data to kafka. Kafka output module consists of Kafka output operator.&nbsp;
 * <p>
 * <br>
 * Ports: <br>
 * <b>Input</b>: Having only single proxy input port. <br>
 * <b>Output</b>: No proxy output port.
 * <br>
 * Properties: <br>
 * <b>topic</b>: Name of the topic from where to write messages.<br>
 * <b>brokerList</b>: List of brokers in the form of Host1:Port1,Host2:Port2,Host3:port3,...<br>
 * <b>partitionCount</b>: Specifies the number of static operator instances. <br>
 * <b>keyField</b>: Specifies the messages writes to Kafka based on this key. <br>
 * <b>properties</b>: specifies the additional producer properties in comma separated
 *          as string in the form of key1=value1,key2=value2,key3=value3,.. <br>
 * <b>isBatchProcessing</b>: Specifies whether want to write messages in batch or not. <br>
 * <b>batchSize</b>: Specifies the batch size.<br>
 * <b></b>
 *
 * <br>
 * </p>
 *
 */
public class KafkaOutputModule implements Module
{
  @NotNull
  private String topic;
  @NotNull
  private String brokerList;
  private int partitionCount = 1;
  private String keyField = "";
  private String properties = "";
  protected boolean isBatchProcessing = true;
  @Min(2)
  protected int batchSize = 200;

  public transient ProxyInputPort<Object> input = new ProxyInputPort<>();
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    POJOKafkaOutputOperator output = dag.addOperator("MessageWriter", new POJOKafkaOutputOperator());
    //Setting the module properties to operator and dag
    output.setTopic(topic);
    Properties prop = new Properties();
    prop.setProperty("metadata.broker.list", brokerList);
    output.setConfigProperties(prop);
    dag.setAttribute(output, Context.OperatorContext.PARTITIONER,
        new StatelessPartitioner<POJOKafkaOutputOperator>(partitionCount));
    output.setKeyField(keyField);
    output.setProperties(properties);
    output.setBatchProcessing(isBatchProcessing);
    output.setBatchSize(batchSize);
    //Setting the proxy port
    input.set(output.inputPort);
  }

  /**
   * Returns the topic name where to write messages
   * @return topic name
   */
  public String getTopic()
  {
    return topic;
  }

  /**
   * Sets the topic name with the given name, to write messages
   * @param topic topic name
   */
  public void setTopic(@NotNull String topic)
  {
    this.topic = topic;
  }

  /**
   * Returns the broker list of kafka clusters
   * @return the broker list
   */
  public String getBrokerList()
  {
    return brokerList;
  }

  /**
   * Sets the broker list with the given list
   * @param brokerList
   */
  public void setBrokerList(@NotNull String brokerList)
  {
    this.brokerList = brokerList;
  }

  /**
   * Returns the number of static operator instances
   * @return partition count
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  /**
   * Sets the number of static operator instances
   * @param partitionCount partition count
   */
  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  /**
   * Returns the key field
   * @return key field
   */
  public String getKeyField()
  {
    return keyField;
  }

  /**
   * Sets the key field which specifies the messages writes to Kafka based on this key.
   * @param keyField keyfield
   */
  public void setKeyField(String keyField)
  {
    this.keyField = keyField;
  }

  /**
   * Return the producer properties
   * @return properties
   */
  public String getProperties()
  {
    return properties;
  }

  /**
   * Sets the properties which specifies the additional producer properties
   * in comma separated as string in the form of key1=value1,key2=value2,key3=value3,..
   * @param properties
   */
  public void setProperties(String properties)
  {
    this.properties = properties;
  }

  /**
   * Specifies whether want to write messages in batch or not.
   * @return isBatchProcessing
   */
  public boolean isBatchProcessing()
  {
    return isBatchProcessing;
  }

  /**
   * Sets the batchProcessing which Specifies whether want to write messages in batch or not.
   * @param batchProcessing is batch processing
   */
  public void setBatchProcessing(boolean batchProcessing)
  {
    isBatchProcessing = batchProcessing;
  }

  /**
   * Returns the batch size
   * @return batch size
   */
  public int getBatchSize()
  {
    return batchSize;
  }

  /**
   * Sets the batch size with the given size
   * @param batchSize batch size
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }
}
