package com.datatorrent.modules.app;


import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.input.AbstractFileSplitter;
import com.datatorrent.module.S3InputModule;
import com.datatorrent.netlet.util.Slice;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.module.KafkaInputModule;

@ApplicationAnnotation(name = "S3BenchmarkApp")
public class S3BenchmarkApp implements StreamingApplication
{
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
        S3InputModule input = dag.addModule("s3InputModule", new S3InputModule());
        DevNull<AbstractFileSplitter.FileMetadata> fileoutput = dag.addOperator("filedevNull", DevNull.class);
        DevNull<BlockMetadata.FileBlockMetadata> blockoutput = dag.addOperator("blockdevNull", DevNull.class);
        DevNull<AbstractBlockReader.ReaderRecord<Slice>> messages = dag.addOperator("msgdevNull", DevNull.class);
        dag.addStream("BlockMessages", input.blocksMetadataOutput, blockoutput.data).setLocality(DAG.Locality.THREAD_LOCAL);
        dag.addStream("FileMessages", input.filesMetadataOutput, fileoutput.data).setLocality(DAG.Locality.THREAD_LOCAL);
        dag.addStream("Messages", input.messages, messages.data).setLocality(DAG.Locality.THREAD_LOCAL);
    }
}