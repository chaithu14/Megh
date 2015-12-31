package com.datatorrent.modules.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.io.input.AbstractFileSplitter;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.module.S3InputModule;

@ApplicationAnnotation(name = "S3DemoApp")
public class S3App implements StreamingApplication
{
  static final String OUT_DATA_FILE = "fileData.txt";
  static final String OUT_METADATA_FILE = "fileMetaData.txt";
  public void populateDAG(DAG dag, Configuration conf)
  {
    S3InputModule module = dag.addModule("s3InputModule", S3InputModule.class);

    AbstractFileOutputOperator<AbstractFileSplitter.FileMetadata> metadataWriter = new MetadataWriter(OUT_METADATA_FILE);
    dag.addOperator("FileMetadataWriter", metadataWriter);

    AbstractFileOutputOperator<AbstractBlockReader.ReaderRecord<Slice>> dataWriter = new FileWriter(OUT_DATA_FILE);
    dag.addOperator("FileDataWriter", dataWriter);

    DevNull<BlockMetadata.FileBlockMetadata> devNull = dag.addOperator("devNull", DevNull.class);

    dag.addStream("FileMetaData", module.filesMetadataOutput, metadataWriter.input);
    dag.addStream("data", module.messages, dataWriter.input);
    dag.addStream("blockMetadata", module.blocksMetadataOutput, devNull.data);
  }
}
