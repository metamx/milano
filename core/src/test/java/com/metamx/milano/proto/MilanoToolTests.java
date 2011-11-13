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
package com.metamx.milano.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.metamx.milano.generated.io.MilanoTypeMetadata;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class MilanoToolTests
{
  private static final Logger log = Logger.getLogger(MilanoToolTests.class);
  private DescriptorProtos.DescriptorProto simpleDescriptorProto;
  private Descriptors.FileDescriptor simpleFileDescriptor;
  private DescriptorProtos.FileDescriptorProto simpleFileDescriptorProto;
  private DescriptorProtos.FileDescriptorSet simpleFileDescriptorSet;
  private MilanoTypeMetadata.TypeMetadata simpleTypeMetadata;
  private String simpleB64Bytes;

  private DescriptorProtos.DescriptorProto completeDescriptorProto;
  private Descriptors.FileDescriptor completeFileDescriptor;
  private DescriptorProtos.FileDescriptorProto completeFileDescriptorProto;
  private DescriptorProtos.FileDescriptorSet completeFileDescriptorSet;
  private MilanoTypeMetadata.TypeMetadata completeTypeMetadata;
  private String completeB64Bytes;

  @Before
  public void setUp() throws Exception
  {
    Descriptors.Descriptor simpleDescriptor = Testing.TestItem.getDescriptor();
    simpleDescriptorProto = simpleDescriptor.toProto();

    simpleFileDescriptor = Testing.getDescriptor();

    simpleFileDescriptorProto = simpleFileDescriptor.toProto();
    DescriptorProtos.FileDescriptorSet.Builder simpleBuilder = DescriptorProtos.FileDescriptorSet.newBuilder();

    for (Descriptors.FileDescriptor fileDescriptor : simpleFileDescriptor.getDependencies()) {
      simpleBuilder.addFile(fileDescriptor.toProto());
    }
    simpleFileDescriptorSet = simpleBuilder.build();

    simpleTypeMetadata = MilanoTypeMetadata.TypeMetadata
        .newBuilder()
        .setTypeName(simpleDescriptor.getName())
        .setTypePackageName(simpleDescriptor.getFile().getPackage())
        .setTypeFileDescriptor(simpleDescriptor.getFile().toProto())
        .build();

    simpleB64Bytes = Base64.encodeBase64String(simpleTypeMetadata.toByteArray());

    Descriptors.Descriptor completeDescriptor = TestingComplete.CompleteTestItem.getDescriptor();
    completeDescriptorProto = completeDescriptor.toProto();

    completeFileDescriptor = TestingComplete.getDescriptor();

    completeFileDescriptorProto = completeFileDescriptor.toProto();
    DescriptorProtos.FileDescriptorSet.Builder completeBuilder = DescriptorProtos.FileDescriptorSet.newBuilder();

    for (Descriptors.FileDescriptor fileDescriptor : completeFileDescriptor.getDependencies()) {
      completeBuilder.addFile(fileDescriptor.toProto());
    }
    completeFileDescriptorSet = completeBuilder.build();

    completeTypeMetadata = MilanoTypeMetadata.TypeMetadata
        .newBuilder()
        .setTypeName(completeDescriptor.getName())
        .setTypePackageName(completeDescriptor.getFile().getPackage())
        .setTypeFileDescriptor(completeDescriptor.getFile().toProto())
        .build();

    completeB64Bytes = Base64.encodeBase64String(completeTypeMetadata.toByteArray());

  }

  @Test
  public void testSimpleTypeMetadataTranslation() throws Exception
  {
    log.debug("Testing TypeMetadata --> Simple");

    MilanoTool milanoTool = MilanoTool.with(simpleTypeMetadata);
    MilanoTypeMetadata.TypeMetadata metadata = milanoTool.getMetadata();
    DescriptorProtos.DescriptorProto descriptorProto = milanoTool.getDescriptorProto();
    Descriptors.Descriptor descriptor = milanoTool.getDescriptor();
    String bytes = milanoTool.getBase64();

    Assert.assertEquals(simpleDescriptorProto, descriptorProto);
    Assert.assertEquals(simpleTypeMetadata, metadata);
    Assert.assertEquals(simpleB64Bytes, bytes);
  }

  @Test
  public void testSimpleB64Translation() throws Exception
  {
    log.debug("Testing B64 --> Simple");

    MilanoTool milanoTool = MilanoTool.withBase64(simpleB64Bytes);
    MilanoTypeMetadata.TypeMetadata metadata = milanoTool.getMetadata();
    DescriptorProtos.DescriptorProto descriptorProto = milanoTool.getDescriptorProto();
    Descriptors.Descriptor descriptor = milanoTool.getDescriptor();
    String bytes = milanoTool.getBase64();

    Assert.assertEquals(simpleTypeMetadata, metadata);
    Assert.assertEquals(simpleDescriptorProto, descriptorProto);
    Assert.assertEquals(simpleB64Bytes, bytes);
  }

  @Test
  public void testSimpleDescriptorProtoTranslation() throws Exception
  {
    log.debug("Testing DescriptorProto --> Simple");

    MilanoTool milanoTool = MilanoTool.with(
        simpleDescriptorProto,
        simpleFileDescriptorProto,
        simpleFileDescriptorSet
    );
    MilanoTypeMetadata.TypeMetadata metadata = milanoTool.getMetadata();
    DescriptorProtos.DescriptorProto descriptorProto = milanoTool.getDescriptorProto();
    String bytes = milanoTool.getBase64();

    Assert.assertEquals(simpleDescriptorProto, descriptorProto);
    Assert.assertEquals(simpleTypeMetadata, metadata);
    Assert.assertEquals(simpleB64Bytes, bytes);
  }

  @Test
  public void testSimpleFileDescriptorTranslation() throws Exception
  {
    log.debug("Testing FileDescriptor --> Simple");

    MilanoTool milanoTool = MilanoTool.with(
        simpleDescriptorProto.getName(),
        simpleFileDescriptor
    );
    MilanoTypeMetadata.TypeMetadata metadata = milanoTool.getMetadata();
    DescriptorProtos.DescriptorProto descriptorProto = milanoTool.getDescriptorProto();
    String bytes = milanoTool.getBase64();


    Assert.assertEquals(simpleDescriptorProto, descriptorProto);
    Assert.assertEquals(simpleTypeMetadata, metadata);
    Assert.assertEquals(simpleB64Bytes, bytes);
  }


  @Test
  public void testCompleteTypeMetadataTranslation() throws Exception
  {
    log.debug("Testing TypeMetadata --> Complete");

    MilanoTool milanoTool = MilanoTool.with(completeTypeMetadata);
    MilanoTypeMetadata.TypeMetadata metadata = milanoTool.getMetadata();
    DescriptorProtos.DescriptorProto descriptorProto = milanoTool.getDescriptorProto();
    String bytes = milanoTool.getBase64();

    Assert.assertEquals(completeDescriptorProto, descriptorProto);
    Assert.assertEquals(completeTypeMetadata, metadata);
    Assert.assertEquals(completeB64Bytes, bytes);
  }

  @Ignore("Doesn't work due to option bug.")
  @Test
  public void testCompleteB64Translation() throws Exception
  {
    log.debug("Testing B64 --> Complete");

    MilanoTool milanoTool = MilanoTool.withBase64(completeB64Bytes);
    MilanoTypeMetadata.TypeMetadata metadata = milanoTool.getMetadata();
    DescriptorProtos.DescriptorProto descriptorProto = milanoTool.getDescriptorProto();
    String bytes = milanoTool.getBase64();

    Assert.assertEquals(completeTypeMetadata, metadata);
    Assert.assertEquals(completeDescriptorProto, descriptorProto);
    Assert.assertEquals(completeB64Bytes, bytes);
  }

  @Test
  public void testCompleteDescriptorProtoTranslation() throws Exception
  {
    log.debug("Testing DescriptorProto --> Complete");

    MilanoTool milanoTool = MilanoTool.with(
        completeDescriptorProto,
        completeFileDescriptorProto,
        completeFileDescriptorSet
    );
    MilanoTypeMetadata.TypeMetadata metadata = milanoTool.getMetadata();
    DescriptorProtos.DescriptorProto descriptorProto = milanoTool.getDescriptorProto();
    String bytes = milanoTool.getBase64();


    Assert.assertEquals(completeDescriptorProto, descriptorProto);
    Assert.assertEquals(completeTypeMetadata, metadata);
    Assert.assertEquals(completeB64Bytes, bytes);
  }

  @Test
  public void testCompleteFileDescriptorTranslation() throws Exception
  {
    log.debug("Testing FileDescriptor --> Complete");

    MilanoTool milanoTool = MilanoTool.with(
        completeDescriptorProto.getName(),
        completeFileDescriptor
    );
    MilanoTypeMetadata.TypeMetadata metadata = milanoTool.getMetadata();
    DescriptorProtos.DescriptorProto descriptorProto = milanoTool.getDescriptorProto();
    String bytes = milanoTool.getBase64();

    Assert.assertEquals(completeDescriptorProto, descriptorProto);
    Assert.assertEquals(completeTypeMetadata, metadata);
    Assert.assertEquals(completeB64Bytes, bytes);
  }

//  @Ignore("Complex, incomplete test")
  @Test
  public void testDynamicDescriptorProto() throws Exception
  {
    DescriptorProtos.DescriptorProto.Builder builder = DescriptorProtos.DescriptorProto.newBuilder();
    builder.addFieldBuilder()
           .setName("test")
           .setNumber(1)
           .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
           .build();
    builder.setName("descriptor");

    DescriptorProtos.DescriptorProto descriptorProto = builder.build();

    DescriptorProtos.FileDescriptorProto fileDescriptorProto = DescriptorProtos.FileDescriptorProto
        .newBuilder()
        .addMessageType(descriptorProto)
        .setName("milano_dynamic_type.proto")
        .setPackage("metamx.milano.dynamic")
        .build();

    MilanoTool milanoTool = MilanoTool.with(
        descriptorProto,
        fileDescriptorProto,
        DescriptorProtos.FileDescriptorSet.getDefaultInstance()
    );
    MilanoTypeMetadata.TypeMetadata descriptorProtoMetadata = milanoTool.getMetadata();
    String descriptorProtoBase64 = milanoTool.getBase64();

    DynamicMessage originalMessage = milanoTool.newBuilder()
                                    .setField(milanoTool.getDescriptor().findFieldByName("test"), "foo!")
                                    .build();

    ByteString byteString = originalMessage.toByteString();

    MilanoTool b64MilanoTool = MilanoTool.withBase64(descriptorProtoBase64);
    MilanoTypeMetadata.TypeMetadata b64Metadata = b64MilanoTool.getMetadata();
    DescriptorProtos.DescriptorProto b64DescriptorProto = b64MilanoTool.getDescriptorProto();
    String b64Base64 = b64MilanoTool.getBase64();

    DynamicMessage dynamicMessage = b64MilanoTool.parseFrom(byteString);

    Assert.assertEquals("foo!", dynamicMessage.getField(b64MilanoTool.getDescriptor().findFieldByName("test")));
    Assert.assertEquals(originalMessage.toString(), dynamicMessage.toString());

    Assert.assertEquals(descriptorProto, b64DescriptorProto);
    Assert.assertEquals(descriptorProtoMetadata, b64Metadata);
    Assert.assertEquals(descriptorProtoBase64, b64Base64);
  }
}
