/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.kafka;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.util.PojoUtils;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * KafkaByteArrayOuputOperator extends from AbstractKafkaOutputOperator and
 * writes the messages to kafka.&nbsp;
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Have only one input port<br>
 * <b>Output</b>: No Output Port <br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * </p>
 *
 * @displayName Kafka Byte Array Output
 * @category Messaging
 * @tags Output operator
 *
 */
public class POJOKafkaOutputOperator extends AbstractKafkaOutputOperator<Object,Object>
{
  @AutoMetric
  private long outputMessagesPerSec;
  @AutoMetric
  private long outputBytesPerSec;

  private long messageCount;
  private long byteCount;
  @NotNull
  private String brokerList;
  private double windowTimeSec;
  private String properties = "";
  private String keyField = "";
  protected boolean isBatchProcessing = true;
  @Min(2)
  protected int batchSize = 200;
  protected transient List<KeyedMessage<Object,Object>> messageList = new ArrayList<>();
  protected transient PojoUtils.Getter keyMethod;
  protected transient Class<?> pojoClass;
  protected transient ProducerConfig producerConfig;

  public final transient DefaultInputPort<Object> inputPort = new DefaultInputPort<Object>() {
    @Override
    public void setup(Context.PortContext context)
    {
      if(context.getAttributes().contains(Context.PortContext.TUPLE_CLASS)) {
        pojoClass = context.getAttributes().get(Context.PortContext.TUPLE_CLASS);
      }
    }

    @Override
    public void process(Object tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * setup producer configuration.
   * @return ProducerConfig
   */
  @Override
  protected ProducerConfig createKafkaProducerConfig(){
    Properties prop = new Properties();
    for (String propString : properties.split(",")) {
      if (!propString.contains("=")) {
        continue;
      }
      String[] keyVal = StringUtils.trim(propString).split("=");
      prop.put(StringUtils.trim(keyVal[0]), StringUtils.trim(keyVal[1]));
    }
    getConfigProperties().setProperty("metadata.broker.list", brokerList);
    getConfigProperties().setProperty("batch.num.messages", String.valueOf(batchSize));

    getConfigProperties().putAll(prop);
    producerConfig = new ProducerConfig(getConfigProperties());
    return producerConfig;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)
      * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
    if(pojoClass != null && keyField != "") {
      try {
        keyMethod = generateGettersForField(pojoClass, keyField);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Field " + keyField + " is invalid: " + e);
      }
    }
    batchSize = producerConfig.batchNumMessages();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    outputMessagesPerSec = 0;
    outputBytesPerSec = 0;
    messageCount = 0;
    byteCount = 0;
  }

  /**
   * Write the incoming tuple to Kafka
   * @param tuple incoming tuple
   */
  protected void processTuple(Object tuple)
  {
    if(keyMethod == null && keyField != "") {
      pojoClass = tuple.getClass();
      try {
        keyMethod = generateGettersForField(pojoClass, keyField);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Field " + keyField + " is invalid: " + e);
      }
    }

    KeyedMessage msg;
    if(keyMethod != null) {
      msg = new KeyedMessage(getTopic(), keyMethod.get(tuple), tuple);
    } else {
      msg = new KeyedMessage(getTopic(), tuple, tuple);
    }

    if (isBatchProcessing) {
      if (messageList.size() == batchSize) {
        getProducer().send(messageList);
        messageList.clear();
      }
      messageList.add(msg);
    } else {
      getProducer().send(msg);
    }
    messageCount++;
    if(tuple instanceof byte[]) {
      byteCount += ((byte[])tuple).length;
    }
  }

  @Override
  public void endWindow()
  {
    if (isBatchProcessing && messageList.size() != 0) {
      getProducer().send(messageList);
      messageList.clear();
    }
    super.endWindow();
    outputBytesPerSec = (long) (byteCount / windowTimeSec);
    outputMessagesPerSec = (long) (messageCount / windowTimeSec);
  }

  private PojoUtils.Getter generateGettersForField(Class<?> klass, String inputFieldName) throws NoSuchFieldException, SecurityException
  {
    Field f = klass.getDeclaredField(inputFieldName);
    Class c = ClassUtils.primitiveToWrapper(f.getType());
    PojoUtils.Getter classGetter = PojoUtils.createGetter(klass, inputFieldName, c);
    return classGetter;
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
   * Return the producer properties
   * @return properties
   */
  public String getProperties()
  {
    return properties;
  }

  /**
   * Specify the additional producer properties in comma separated as string
   * @param properties Given properties as string
   */
  public void setProperties(String properties)
  {
    this.properties = properties;
  }

  /**
   * Specifies whether want to write in batch or not.
   * @return isBatchProcessing
   */
  public boolean isBatchProcessing()
  {
    return isBatchProcessing;
  }

  /**
   * Specifies whether want to write in batch or not.
   * @param batchProcessing given batchProcessing
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
   * Sets the batch size
   * @param batchSize batch size
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  /**
   * Returns the key field
   * @return the key field
   */
  public String getKeyField()
  {
    return keyField;
  }

  /**
   * Sets the key field which specifies the messages writes to Kafka based on this key.
   * @param keyField the key field
   */
  public void setKeyField(String keyField)
  {
    this.keyField = keyField;
  }
}
