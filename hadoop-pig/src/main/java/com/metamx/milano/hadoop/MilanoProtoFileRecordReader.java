/**
 * Copyright (C) 2011 Metamarkets http://metamx.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.metamx.milano.hadoop;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.metamx.milano.generated.io.MilanoTypeMetadata;
import com.metamx.milano.io.MilanoProtoFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 *
 */
public class MilanoProtoFileRecordReader extends RecordReader<String, Message>
{
  private static final Logger log = Logger.getLogger(MilanoProtoFileRecordReader.class);

  private long end;
  private long start;
  private boolean more;
  private Message value;
  private static final int DEFAULT_BUFFER_SIZE = 4096;
  private MilanoProtoFile.Reader in;
  private MilanoTypeMetadata.TypeMetadata metadata;
  private InputSplit split;
  private TaskAttemptContext context;
  private Message.Builder builder;
  private ExtensionRegistry extensionRegistry;

  public MilanoProtoFileRecordReader(
      InputSplit split,
      TaskAttemptContext context,
      Message.Builder builder,
      ExtensionRegistry extensionRegistry
  )
  {
    this.split = split;
    this.context = context;
    this.builder = builder;
    this.extensionRegistry = extensionRegistry;
  }


  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
  {
    FileSplit fileSplit = (FileSplit) split;
    Configuration conf = context.getConfiguration();
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
//    fs.setVerifyChecksum(false);

    //Passing a null builder to createReader works as long as the file has metadata.
    in = MilanoProtoFile.createReader(
        fs.open(path, DEFAULT_BUFFER_SIZE),
        builder,
        extensionRegistry,
        fileSplit.getLength()
    );

    // This should always be not null.
    metadata = in.getMetadata();
    assert metadata != null;

    // We keep statistics on how much has been read to be able to report progress.
    start = in.getBytesRead();
    end = fileSplit.getLength();
    more = start < end;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    if (!more) {
      return false;
    }

    long left = in.getBytesLeft();
    if (left > 0) {
      value = in.read();
    } else {
      value = null;
      more = false;
    }

    return more;
  }

  @Override
  public String getCurrentKey() throws IOException, InterruptedException
  {
    //Currently we do nothing with this. This could possibly change in the future.
    return null;
  }

  @Override
  public Message getCurrentValue() throws IOException, InterruptedException
  {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getBytesRead() - start) / (float) (end - start));
    }
  }

  public MilanoTypeMetadata.TypeMetadata getMetadata()
  {
    return metadata;
  }

  @Override
  public void close() throws IOException
  {
    in.close();
  }
}
