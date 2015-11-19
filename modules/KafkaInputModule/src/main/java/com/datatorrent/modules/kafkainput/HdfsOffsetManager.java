package com.datatorrent.modules.kafkainput;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.contrib.kafka.KafkaPartition;
import com.datatorrent.contrib.kafka.OffsetManager;

/**
 * Created by chaitanya on 13/11/15.
 */
public class HdfsOffsetManager implements OffsetManager
{
  private static final Logger LOG = LoggerFactory.getLogger(HdfsOffsetManager.class);

  @NotNull
  private String offsetPath;

  @NotNull
  private String delimiter;

  @NotNull
  private String topicName;

  public String getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  public String getOffsetPath() {
    return offsetPath;
  }

  public void setOffsetPath(String offsetPath) {
    this.offsetPath = offsetPath;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public String getTopicName() { return topicName; }

  @Override
  public Map<KafkaPartition, Long> loadInitialOffsets()
  {
    Map<KafkaPartition, Long> offsetMap = new HashMap<KafkaPartition, Long>();
    if(offsetPath == null) {
      return offsetMap;
    }
    Configuration configuration = new Configuration();
    Path dataPath = new Path(offsetPath);
    FileSystem fs;
    InputStreamReader stream = null;
    BufferedReader br = null;
    try {
      fs = getFileSystem(configuration, dataPath);
      if(fs.exists(dataPath) && fs.isFile(dataPath)) {
        LOG.info("reading offset file " + dataPath);
        stream = new InputStreamReader(fs.open(dataPath));
        br = new BufferedReader(stream);
        String line;
        line = br.readLine();
        while (line != null) {
          StringTokenizer st = new StringTokenizer(line, delimiter);
          int count = st.countTokens();
          while (st.hasMoreTokens()) {
            if (count == 2) {
              offsetMap.put(new KafkaPartition(topicName, Integer.parseInt(st.nextToken())), Long.parseLong(st.nextToken()));
            } else if (count == 3) {
              offsetMap.put(new KafkaPartition(st.nextToken(), topicName, Integer.parseInt(st.nextToken())), Long.parseLong(st.nextToken()));
            }
          }
          line = br.readLine();
        }
      }
      return offsetMap;
    } catch (IOException e) {
      throw new RuntimeException("Error reading offset file [ " + offsetPath + " ]" + e);
    } finally {
      try {
        if (stream != null) {
          stream.close();
        }
      } catch (Exception e) {
      }
      try {
        if (br != null) {
          br.close();
        }
      } catch (Exception e) {
      }
    }
  }

  private FileSystem getFileSystem(final Configuration configuration, final Path dataPath)
    throws IOException {
    return FileSystem.get(dataPath.toUri(), configuration);
  }

  @Override
  public void updateOffsets(Map<KafkaPartition, Long> kafkaPartitionLongMap)
  {
    Configuration configuration = new Configuration();
    Path dataPath = new Path(offsetPath);
    FileSystem fs = null;
    FSDataOutputStream out = null;
    try {
      fs = getFileSystem(configuration, dataPath);
      if (fs.exists(dataPath) && fs.isFile(dataPath)) {
        LOG.info("writing offsets to file  {}", dataPath);

        out = fs.create(dataPath, true);
        for (Map.Entry<KafkaPartition, Long> entry : kafkaPartitionLongMap.entrySet()) {
          String str = entry.getKey().getClusterId() + getDelimiter() + entry.getKey().getPartitionId() + getDelimiter() + entry.getValue() + "\n";
          out.writeBytes(str);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing offset file [ " + offsetPath + " ]" + e);
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } catch (Exception e) {
      }
      try {
        if (fs != null) {
          fs.close();
        }
      } catch (Exception e) {
      }
    }
  }
}
