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
package com.metamx.milano.io;

import com.metamx.milano.ProtoTestObjects;
import com.metamx.milano.generated.io.MilanoTypeMetadata;
import com.metamx.milano.proto.MilanoTool;
import com.metamx.milano.proto.Testing;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 *
 */
public class MilanoProtoFileTests
{
  final ProtoTestObjects protoTestObjects = new ProtoTestObjects();

  @Test
  public void testWriteFile() throws Exception
  {
    // Use a ByteArrayOutputStream to proxy a real file.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    MilanoTypeMetadata.TypeMetadata metadata = MilanoTool.with("TestItem", Testing.getDescriptor()).getMetadata();
    MilanoProtoFile.Writer writer = MilanoProtoFile.createWriter(outputStream, metadata);

    for (int i = 0; i < protoTestObjects.getTestItems().size(); i++) {
      writer.write(protoTestObjects.getTestItem(i));
    }

    writer.close();

    // Use metadata to decode file.
    MilanoProtoFile.Reader reader = MilanoProtoFile.createReader(new ByteArrayInputStream(outputStream.toByteArray()));

    int i = 0;
    while (!reader.empty()) {
      protoTestObjects.compareMessages(protoTestObjects.getTestItem(i), reader.read());
      i++;
    }
    reader.close();

    // Use builder to decode file.
    reader = MilanoProtoFile.createReader(new ByteArrayInputStream(outputStream.toByteArray()));

    i = 0;
    while (!reader.empty()) {
      protoTestObjects.compareMessages(protoTestObjects.getTestItem(i), reader.read());
      i++;
    }
    reader.close();
  }

  @Test
  public void testWriteFileNoMetadata() throws Exception
  {
    // Use a ByteArrayOutputStream to proxy a real file.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    MilanoTypeMetadata.TypeMetadata metadata = MilanoTool.with("TestItem", Testing.getDescriptor()).getMetadata();
    MilanoProtoFile.Writer writer = MilanoProtoFile.createWriter(outputStream);

    for (int i = 0; i < protoTestObjects.getTestItems().size(); i++) {
      writer.write(protoTestObjects.getTestItem(i));
    }

    writer.close();

    // Try to use metadata to decode file.
    Boolean failed = false;
    try {
      MilanoProtoFile.Reader reader = MilanoProtoFile.createReader(new ByteArrayInputStream(outputStream.toByteArray()));
    }
    catch (IllegalStateException e) {
      failed = true;
    }

    Assert.assertTrue("No exception was thrown for missing metadata.", failed);

    // Use builder to decode file.
    MilanoProtoFile.Reader reader = MilanoProtoFile.createReader(
        new ByteArrayInputStream(outputStream.toByteArray()),
        Testing.TestItem.newBuilder()
    );

    int i = 0;
    while (!reader.empty()) {
      protoTestObjects.compareMessages(protoTestObjects.getTestItem(i), reader.read());
      i++;
    }
    reader.close();
  }
}
