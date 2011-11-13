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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 *
 */
public class MilanoProtoFileInputFormat extends FileInputFormat<String, Message>
{
  private static Logger log = Logger.getLogger(MilanoProtoFileInputFormat.class);
  private Configuration conf;
  private Message.Builder builder = null;
  private ExtensionRegistry extensionRegistry = null;

  @Override
  public RecordReader<String, Message> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException
  {
    log.debug("Creating RecordReader");
    return new MilanoProtoFileRecordReader(split, context, builder, extensionRegistry);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename)
  {
    // This functionality requires support by MilanoProtoFile.
    return false;
  }

  public void setBuilder(Message.Builder builder)
  {
    log.debug("Received builder");
    this.builder = builder;
  }

  public void setExtensionRegistry(ExtensionRegistry extensionRegistry)
  {
    log.debug("Received the ExtensionRegistry");
    this.extensionRegistry = extensionRegistry;
  }
}
