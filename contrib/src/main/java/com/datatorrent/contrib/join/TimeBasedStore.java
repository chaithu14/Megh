/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.join;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.util.concurrent.MoreExecutors;

import com.datatorrent.contrib.hdht.HDHTStore;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.netlet.util.Slice;

/**
 * Base implementation of time based store for key-value pair tuples.
 *
 * @param <T>
 */
public class TimeBasedStore<T extends TimeEvent>
{
  private static transient final Logger logger = LoggerFactory.getLogger(TimeBasedStore.class);
  private long expiryTime;

  private HDHTStore store;

  private long BUCKET = 1L;
  static long BucketId = 1l;

  public TimeBasedStore(long expiryTime)
  {
    this.expiryTime = expiryTime;
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    BUCKET = BucketId;
    BucketId++;
    hdsFile.setBasePath("operator/" + BucketId);
    store = new HDHTStore();

    store.setFileStore(hdsFile);
    store.setMaxFileSize(1); // limit to single entry per file
    store.setFlushSize(0); // flush after every key

    store.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush on endWindow
  }

  public void setup()
  {
    store.setup(null);
  }

  public Object getValidTuples(Object tuple)
  {
    Object key = ((TimeEvent)tuple).getEventKey();
    byte[] key2bytes = key.toString().getBytes();
    Slice keySlice = new Slice(key2bytes, 0, key2bytes.length);
    byte[] value = store.getUncommitted(BUCKET, keySlice);
    Long startTime = ((TimeEvent)tuple).getTime();
    if (value != null) {
      Input lInput = new Input(value);
      Kryo kryo = new Kryo();
      List<Object> t = (List<Object>)kryo.readObject(lInput, ArrayList.class);
      List<Object> validTuples = new ArrayList<Object>();
      for (Object rightTuple : t) {
        if (Math.abs(startTime - ((TimeEvent)rightTuple).getTime()) <= expiryTime) {
          validTuples.add(rightTuple);
        }
      }
      return validTuples;
    }
    return null;
  }

  public void committed(long windowId)
  {
    store.committed(windowId);
  }

  public void checkpointed(long windowId)
  {
    store.checkpointed(windowId);
  }


  public Boolean put(Object tuple)
  {
    Object key = ((TimeEvent)tuple).getEventKey();
    byte[] key2bytes = key.toString().getBytes();
    Slice keySlice = new Slice(key2bytes, 0, key2bytes.length);
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bos = null;
    byte[] value = store.getUncommitted(BUCKET, keySlice);
    if (value == null) {
      List<Object> ob = new ArrayList<Object>();
      ob.add(tuple);

      bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, ob);
      output.close();
    } else {
      Input lInput = new Input(value);
      List<Object> t = (List<Object>)kryo.readObject(lInput, ArrayList.class);
      t.add(tuple);
      bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, t);
      output.close();
    }
    try {
      store.put(BUCKET, keySlice, bos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

}
