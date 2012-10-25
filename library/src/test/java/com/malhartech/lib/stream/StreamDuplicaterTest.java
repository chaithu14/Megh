/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestCountSink;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.StreamDuplicater}<p>
 * Benchmarks: Currently does about ?? Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class StreamDuplicaterTest {

    private static Logger log = LoggerFactory.getLogger(StreamDuplicaterTest.class);

    /**
     * Test oper pass through. The Object passed is not relevant
     */
    @Test
    public void testNodeProcessing() throws Exception
    {
      StreamDuplicater oper = new StreamDuplicater();
      TestCountSink mergeSink1 = new TestCountSink();
      TestCountSink mergeSink2 = new TestCountSink();

      oper.out1.setSink(mergeSink1);
      oper.out2.setSink(mergeSink2);
      oper.setup(new OperatorConfiguration());

      oper.beginWindow();
      int numtuples = 50000000;
      Integer ival = new Integer(numtuples);
      Integer input = new Integer(0);
      // Same input object can be used as the oper is just pass through
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(input);
      }

      oper.endWindow();

      // One for each key
      Assert.assertEquals("number emitted tuples", ival, mergeSink1.numTuples);
      Assert.assertEquals("number emitted tuples", ival, mergeSink2.numTuples);
      log.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", mergeSink1.numTuples+mergeSink2.numTuples));
    }
}
