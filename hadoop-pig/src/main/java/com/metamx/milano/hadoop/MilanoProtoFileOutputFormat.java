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

import com.google.protobuf.Message;
import com.metamx.milano.generated.io.MilanoTypeMetadata;
import com.metamx.milano.io.MilanoProtoFile;
import com.metamx.milano.proto.MilanoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Pattern;

/**
 *
 */
public class MilanoProtoFileOutputFormat<K> extends FileOutputFormat<K, Message>
{
  private Logger log = Logger.getLogger(MilanoProtoFileOutputFormat.class);
  private MilanoTypeMetadata.TypeMetadata metadata;

  public MilanoProtoFileOutputFormat()
  {
    log.debug(String.format("OutputFormat created"));
  }

  /**
   * Returns the {@link MilanoTypeMetadata.TypeMetadata} object associated with this OutputFormat.
   *
   * @return A TypeMetadata
   */
  public MilanoTypeMetadata.TypeMetadata getMetadata()
  {
    return metadata;
  }

  /**
   * Set the {@link MilanoTypeMetadata.TypeMetadata} for this OutputFormat.
   * This is required if you want to be able to dynamically deserialize the file.
   *
   * @param metadata A TypeMetadata representing the {@link Message} type to be stored.
   */
  public void setMetadata(MilanoTypeMetadata.TypeMetadata metadata)
  {
    this.metadata = metadata;
  }

  /**
   * Retrieve a record writer for this RecordWriter. There are three config properties that are supported:
   * com.metamx.milano.hadoop.filePrefix -- A string to prefix the written file names with.
   * com.metamx.milano.hadoop.filePath   -- A string to postfix on the path. This lets you specify a subdirectory in which to put the files.
   * com.metamx.milano.proto.descriptor.base64 -- A string representing a base64 encoded DescriptorProto converted to bytes.
   * This is overridden if the metadata has already been set.
   *
   * @param job The {@link TaskAttemptContext} to use. See above for specific options.
   *
   * @return A {@link RecordWriter}
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordWriter<K, Message> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException
  {
    log.debug(String.format("Retrieving record writer"));
    Configuration conf = job.getConfiguration();

    String prefix = conf.get("com.metamx.milano.hadoop.filePrefix", "");
    String path = conf.get("com.metamx.milano.hadoop.filePath", ".");

    if (metadata == null) {
      String descriptorBytes = conf.get("com.metamx.milano.proto.descriptor.base64");
      if (descriptorBytes != null) {
        metadata = MilanoTool.withBase64(descriptorBytes).getMetadata();
      }
    }

    String filename = "";
    if (!prefix.equals("")) {
      filename = filename.concat(prefix + "_");
    }
    filename = filename.concat(job.getTaskAttemptID().getTaskID().toString());
    Path directory = new Path(((FileOutputCommitter) getOutputCommitter(job)).getWorkPath(), path);

    Path file = new Path(directory, filename);
    FileSystem fs = file.getFileSystem(conf);

    final OutputStream outputStream = fs.create(file);

    return new RecordWriter<K, Message>()
    {
      private MilanoProtoFile.Writer writer = MilanoProtoFile.createWriter(outputStream, metadata);

      @Override
      public void write(K key, Message value) throws IOException, InterruptedException
      {
        writer.write(value);
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException
      {
        writer.flush();
        writer.close();
        log.debug("Closed Writer");
      }
    };
  }

  /**
   * Returns a {@link Pattern} which will match files that this generates with captures for the following fields:
   * 1. Instance ID: 12 digits representing a timestamp
   * 2. Job ID:      4 digits representing the job
   * 3. Map/Reduce:  'm' or 'r' Map or Reduce
   * 4. Attempt ID:  6 digits representing the attempt
   *
   * @param conf       The job configuration
   * @param filePrefix The prefix to match files on.
   *
   * @return A Pattern object with the above properties.
   */
  public static Pattern getFilePatternMatcher(Configuration conf, String filePrefix)
  {
    filePrefix = (filePrefix.length() == 0) ? "" : filePrefix + "_";
    return Pattern.compile("^" + filePrefix + "task_(\\d{12}|local)_(\\d{4})_([rm])_(\\d{6})*$");
  }
}
