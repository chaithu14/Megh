package com.datatorrent.modules.app;

import java.util.Date;
import java.util.Random;
import java.util.UUID;

import com.datatorrent.contrib.util.TupleGenerator;

/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */


public class PojoTupleGenerator extends TupleGenerator<TestPojo>
{
  private static int seq = 0;
  private int tupleSize = 0;
  private final Random random = new Random();

  public PojoTupleGenerator()
  {

  }

  public PojoTupleGenerator(Class<TestPojo> tupleClass)
  {
    super(tupleClass);
  }

  public PojoTupleGenerator(Class<TestPojo> tupleClass, int tupleSize)
  {
    super(tupleClass);
    this.tupleSize = tupleSize;
  }

  @Override
  public TestPojo getNextTuple()
  {
    TestPojo pojo = super.getNextTuple();
    pojo.setId(UUID.randomUUID());
    pojo.setAge(seq % 80);
    pojo.setLastname("name" + seq);
    pojo.setLast_visited(new Date());
    pojo.setTest(seq % 2 == 0 ? true : false);
    pojo.setDoubleValue((double)seq);
    pojo.setFloatValue((float)seq);
    byte[] b = new byte[tupleSize];
    random.nextBytes(b);
    pojo.setByteData(b);
    seq++;
    return pojo;
  }
}
