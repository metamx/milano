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
import com.metamx.milano.ProtoTestObjects;
import com.metamx.milano.generated.io.MilanoTypeMetadata;
import com.metamx.milano.io.MilanoProtoFile;
import com.metamx.milano.proto.MilanoTool;
import com.metamx.milano.proto.Testing;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class MilanoProtoFileInputFormatTests
{
  private ProtoTestObjects protoTestObjects = new ProtoTestObjects();
  private Path readFile;
  private Path readFileNoMetadata;

  @Before
  public void setUp() throws Exception
  {
    readFile = new Path(protoTestObjects.getWorkingPath(), "milano-proto-read.test");
    MilanoTypeMetadata.TypeMetadata metadata =
        MilanoTool.with(
            Testing.TestItem
                .getDescriptor()
                .getName(),
            Testing.getDescriptor()
        ).getMetadata();

    MilanoProtoFile.Writer writer = MilanoProtoFile.createWriter(protoTestObjects.getFs().create(readFile), metadata);

    for (int i = 0; i < protoTestObjects.getTestItems().size(); i++) {
      writer.write(protoTestObjects.getTestItem(i));
    }

    writer.close();

    readFileNoMetadata = new Path(protoTestObjects.getWorkingPath(), "milano-proto-read-no-metadata.test");

    writer = MilanoProtoFile.createWriter(protoTestObjects.getFs().create(readFileNoMetadata));

    for (int i = 0; i < protoTestObjects.getTestItems().size(); i++) {
      writer.write(protoTestObjects.getTestItem(i));
    }

    writer.close();
  }

  @Test
  public void testReadFile() throws Exception
  {
    MilanoProtoFileInputFormat inputFormat = new MilanoProtoFileInputFormat();

    FileSplit split = new FileSplit(readFile, 0, protoTestObjects.getFs().getFileStatus(readFile).getLen(), null);
    org.apache.hadoop.mapreduce.RecordReader<String, Message> recordReader = inputFormat.createRecordReader(
        split,
        protoTestObjects.getContext()
    );
    recordReader.initialize(split, protoTestObjects.getContext());

    for (int i = 0; i < protoTestObjects.getTestItems().size(); i++) {
      Assert.assertTrue("Fewer objects than expected.", recordReader.nextKeyValue());
      Message message = recordReader.getCurrentValue();

      protoTestObjects.compareMessages(protoTestObjects.getTestItem(i), message);
    }

    recordReader.close();
  }

  @Test
  public void testReadFileNoMetadata() throws Exception
  {
    MilanoProtoFileInputFormat inputFormat = new MilanoProtoFileInputFormat();
    inputFormat.setBuilder(Testing.TestItem.newBuilder());

    FileSplit split = new FileSplit(readFile, 0, protoTestObjects.getFs().getFileStatus(readFile).getLen(), null);
    org.apache.hadoop.mapreduce.RecordReader<String, Message> recordReader = inputFormat.createRecordReader(
        split,
        protoTestObjects.getContext()
    );
    recordReader.initialize(split, protoTestObjects.getContext());

    for (int i = 0; i < protoTestObjects.getTestItems().size(); i++) {
      Assert.assertTrue("Fewer objects than expected.", recordReader.nextKeyValue());
      Message message = recordReader.getCurrentValue();

      protoTestObjects.compareMessages(protoTestObjects.getTestItem(i), message);
    }

    recordReader.close();
  }
}
