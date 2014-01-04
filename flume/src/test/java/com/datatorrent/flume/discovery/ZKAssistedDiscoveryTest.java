/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.discovery;

import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.flume.Context;

import com.datatorrent.flume.discovery.Discovery.Service;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class ZKAssistedDiscoveryTest
{

  public ZKAssistedDiscoveryTest()
  {
  }

  @Test
  public void testSerialization() throws Exception
  {
    ZKAssistedDiscovery discovery = new ZKAssistedDiscovery();
    discovery.configure(new Context());
    ServiceInstance<byte[]> instance = discovery.getInstance(new Service<byte[]>()
    {

      @Override
      public String getHost()
      {
        return "localhost";
      }

      @Override
      public int getPort()
      {
        return 8080;
      }

      @Override
      public byte[] getPayload()
      {
        return null;
      }

      @Override
      public String getId()
      {
        return "localhost8080";
      }

    });
    InstanceSerializer<byte[]> instanceSerializer = discovery.getInstanceSerializerFactory().getInstanceSerializer(new TypeReference<ServiceInstance<byte[]>>()
    {
    });
    byte[] serialize = instanceSerializer.serialize(instance);
    logger.debug("serialized json = {}", new String(serialize));
    ServiceInstance<byte[]> deserialize = instanceSerializer.deserialize(serialize);
    assertArrayEquals("Metadata", instance.getPayload(), deserialize.getPayload());
  }

  private static final Logger logger = LoggerFactory.getLogger(ZKAssistedDiscoveryTest.class);
}
