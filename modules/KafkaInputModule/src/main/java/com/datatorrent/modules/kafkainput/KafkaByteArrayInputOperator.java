package com.datatorrent.modules.kafkainput;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.tuple.MutablePair;

import com.datatorrent.contrib.kafka.AbstractKafkaSinglePortInputOperator;

import kafka.message.Message;

public class KafkaByteArrayInputOperator extends AbstractKafkaSinglePortInputOperator<MutablePair<String,byte[]>>
{
  /**
   * Implement abstract method of AbstractKafkaSinglePortInputOperator
   * @param message
   * @return
   */
  @Override
  public MutablePair<String,byte[]> getTuple(Message message)
  {
    byte[] bytes = null;
    try {
      ByteBuffer buffer = message.payload();
      bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
    }
    catch (Exception ex) {
      return null;
    }
    return new MutablePair<String,byte[]>(consumer.getTopic(),bytes);
  }
}
