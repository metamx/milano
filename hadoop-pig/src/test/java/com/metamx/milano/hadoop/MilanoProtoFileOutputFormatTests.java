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

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.metamx.milano.ProtoTestObjects;
import com.metamx.milano.generated.io.MilanoTypeMetadata;
import com.metamx.milano.proto.MilanoTool;
import com.metamx.milano.proto.Testing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

/**
 *
 */
public class MilanoProtoFileOutputFormatTests
{
  private final ProtoTestObjects protoTestObjects = new ProtoTestObjects();

  @Test
  public void testBuildEmptyProtoFile() throws Exception
  {
    MilanoProtoFileOutputFormat outputFormat = new MilanoProtoFileOutputFormat();
    @SuppressWarnings("unchecked")
    RecordWriter<String, Message> writer = outputFormat.getRecordWriter(protoTestObjects.getContext());


    writer.close(protoTestObjects.getContext());
  }

  @Test
  public void testBuildAndReadProtoFile() throws Exception
  {
    MilanoProtoFileOutputFormat outputFormat = new MilanoProtoFileOutputFormat();

    MilanoTypeMetadata.TypeMetadata.Builder metadata =
        MilanoTool.with(
            Testing.TestItem.getDescriptor().getName(),
            Testing.getDescriptor()
        ).getMetadata().toBuilder();

    metadata.addFileMetadata(
        MilanoTypeMetadata.FileMetadata.newBuilder().setKey("Key 1").setValue(
            ByteString.copyFromUtf8("Value 1")
        )
    );

    metadata.addFileMetadata(
        MilanoTypeMetadata.FileMetadata.newBuilder().setKey("Key 2").setValue(
            ByteString.copyFromUtf8("Value 2")
        )
    );

    outputFormat.setMetadata(metadata.build());

    TaskAttemptContext context = protoTestObjects.getContext();
    Configuration conf = context.getConfiguration();

    @SuppressWarnings("unchecked")
    RecordWriter<String, Message> writer = outputFormat.getRecordWriter(context);

    for (int i = 0; i < protoTestObjects.getTestItems().size(); i++) {
      writer.write("dummy", protoTestObjects.getTestItem(i));
    }

    writer.close(protoTestObjects.getContext());
  }
}
