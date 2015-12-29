package com.datatorrent.modules.app;

import java.util.concurrent.atomic.AtomicInteger;

import com.datatorrent.contrib.util.POJOTupleGenerateOperator;
import com.datatorrent.contrib.util.TupleGenerator;

public class PojoTuplesGeneratorOperator extends POJOTupleGenerateOperator<TestPojo>
{
  private int tupleSize = 0;
  private int batchNum = 5;
  public PojoTuplesGeneratorOperator()
  {
    super(TestPojo.class);
  }
  private AtomicInteger emitedTuples = new AtomicInteger(0);
  @Override
  protected TupleGenerator<TestPojo> createTupleGenerator()
  {
    return new PojoTupleGenerator(TestPojo.class, tupleSize);
  }

  public int getTupleSize()
  {
    return tupleSize;
  }

  public void setTupleSize(int tupleSize)
  {
    this.tupleSize = tupleSize;
  }

  public void endWindow() {
    emitedTuples.set(0);
  }

  @Override
  public void emitTuples()
  {
    final int theTupleNum = getTupleNum();

    for( int i=0; i<batchNum; ++i )
    {
      int count = emitedTuples.get();
      if( count >= theTupleNum )
        return;

      if( emitedTuples.compareAndSet(count, count+1) )
      {
        TestPojo tuple = getNextTuple();
        outputPort.emit ( tuple );
        //System.out.println("Pojo : " + i + " -> " + tuple.toString());
        tupleEmitted( tuple );

        if( count+1 == theTupleNum )
        {
          tupleEmitDone();
          return;
        }
      }

    }
  }

}
