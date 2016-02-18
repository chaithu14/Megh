package com.datatorrent.app;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kinesis.AbstractKinesisInputOperator;
import com.datatorrent.contrib.kinesis.KinesisStringInputOperator;
import com.datatorrent.contrib.kinesis.KinesisStringOutputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="KinesisToS3App")
public class Application implements StreamingApplication
{
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
        RandomEventGenerator random = dag.addOperator("GenerateData", new RandomEventGenerator());
        random.setTuplesBlast(5);
        KinesisStringOutputOperator output = dag.addOperator("ToKinesis", new KinesisStringOutputOperator());
        output.setBatchSize(50);

        dag.addStream("DataToKinesis", random.string_data, output.inputPort);

        KinesisStringInputOperator inputOp = dag.addOperator("FromKinesis", new KinesisStringInputOperator());
        inputOp.getConsumer().setRecordsLimit(600);
        inputOp.setStrategy(AbstractKinesisInputOperator.PartitionStrategy.MANY_TO_ONE.toString());

        ConsoleOutputOperator console = dag.addOperator("WriteToConsole", new ConsoleOutputOperator());

        dag.addStream("KinesisTOConsole", inputOp.outputPort, console.input);
    }
}