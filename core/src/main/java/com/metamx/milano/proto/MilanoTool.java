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

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.metamx.milano.generated.io.MilanoTypeMetadata;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * This is a tool to manage TypeMetadata, DescriptorProto, Descriptor, and Base64 Strings.
 * <p/>
 * This has several problems that need to be worked out:
 * * TODO: File Information isn't stored or passed on in some cases. This causes some parts of the message to be restored as UnknownFields.
 * * TODO: Needs tests for several cases including the file information problem above and conversion from any input type to any output type.
 */
public final class MilanoTool
{
  private static Logger log = Logger.getLogger(MilanoTool.class);

  private Descriptors.Descriptor descriptor;
  private Descriptors.FileDescriptor fileDescriptor;
  private MilanoTypeMetadata.TypeMetadata typeMetadata;
  private String base64;

  /**
   * Build a MilanoTool from the messageName and fileDescriptor
   *
   * @param messageName    The name of the message to use in the tool.
   * @param fileDescriptor A FileDescriptor containing messageName.
   *
   * @return A MilanoTool of the message.
   */
  public static MilanoTool with(
      final String messageName,
      final Descriptors.FileDescriptor fileDescriptor
  )
  {
    return with(
        fileDescriptor.findMessageTypeByName(messageName).toProto(),
        fileDescriptor.toProto(),
        dehydrateFileDescriptor(fileDescriptor)
    );
  }

  /**
   * Build a MilanoTool from a DescriptorProto, FileDescriptorProto, and a FileDescriptorSet representing
   * the dependencies.
   *
   * @param descriptorProto     The Descriptor for the message.
   * @param fileDescriptorProto The FileDescriptorProto containing descriptorProto.
   * @param dependencies        A possible empty FileDescriptorSet containing the dependencies for fileDescriptorProto.
   *
   * @return A MilanoTool of the message.
   */
  public static MilanoTool with(
      final DescriptorProtos.DescriptorProto descriptorProto,
      final DescriptorProtos.FileDescriptorProto fileDescriptorProto,
      final DescriptorProtos.FileDescriptorSet dependencies
  )
  {
    HashSet<String> fileSet = new HashSet<String>();
    DescriptorProtos.FileDescriptorSet.Builder storedFileSetBuilder = DescriptorProtos.FileDescriptorSet.newBuilder();

    if (dependencies != null) {
      for (DescriptorProtos.FileDescriptorProto fileProto : dependencies.getFileList()) {
        log.debug(String.format("Found dependency [%s] in package [%s]", fileProto.getName(), fileProto.getPackage()));
        fileSet.add(fileProto.getName());

        if (fileProto.getName().equals("descriptor.proto") && fileProto.getPackage().equals("google.protobuf")) {
          continue;
        }
        storedFileSetBuilder.addFile(fileProto);
      }
    }

    for (String dependency : fileDescriptorProto.getDependencyList()) {
      log.debug(String.format("Type requires dependency: %s", dependency));
      if (!fileSet.contains(dependency) && !dependency.equals("descriptor.proto")) {
        throw new IllegalStateException(
            String.format(
                "File requires dependency [%s] that does not exist.",
                dependency
            )
        );
      }
    }

    MilanoTypeMetadata.TypeMetadata.Builder typeMetadataBuilder = MilanoTypeMetadata.TypeMetadata
        .newBuilder()
        .setTypeName(descriptorProto.getName())
        .setTypePackageName(fileDescriptorProto.getPackage())
        .setTypeFileDescriptor(fileDescriptorProto);


    if (storedFileSetBuilder.getFileCount() > 0) {
      typeMetadataBuilder.setTypeDependencies(storedFileSetBuilder.build());
    }

    return new MilanoTool(typeMetadataBuilder.build());
  }

  /**
   * This takes a FileDescriptor and recursively determines all the file dependencies and puts them in a
   * FileDescriptorSet while excluding the ProtoBuf base objects.
   *
   * @param fileDescriptor The FileDescriptor to dehydrate.
   *
   * @return A FileDescriptorSet containing all the dependencies for fileDescriptor.
   */
  private static DescriptorProtos.FileDescriptorSet dehydrateFileDescriptor(final Descriptors.FileDescriptor fileDescriptor)
  {
    DescriptorProtos.FileDescriptorSet.Builder setBuilder = DescriptorProtos.FileDescriptorSet.newBuilder();

    for (Descriptors.FileDescriptor file : fileDescriptor.getDependencies()) {
      if (!file.getName().equals("descriptor.proto") && !file.getPackage().equals("google.protobuf")) {
        setBuilder.mergeFrom(dehydrateFileDescriptor(file));
      }
    }

    return setBuilder.build();
  }

  /**
   * This builds a FileDescriptor from a FileDescriptorProto and a FileDescriptorSet of it's dependencies.
   *
   * @param fileDescriptorProto The FileDescriptorProto to turn into a FileDescriptor.
   * @param fileDescriptorSet   The, possibly empty, FileDescriptorSet of dependencies.
   *
   * @return A new FileDescriptor built from fileDescriptorProto
   *
   * @throws Descriptors.DescriptorValidationException
   *          Thrown when either a dependency or the fileDescriptorProto itself can not be instantiated by ProtoBuf.
   */
  private static Descriptors.FileDescriptor rehydrateFileDescriptor(
      final DescriptorProtos.FileDescriptorProto fileDescriptorProto,
      final DescriptorProtos.FileDescriptorSet fileDescriptorSet
  ) throws Descriptors.DescriptorValidationException
  {
    HashMap<String, DescriptorProtos.FileDescriptorProto> fileDescriptorProtoMap = new HashMap<String, DescriptorProtos.FileDescriptorProto>();
    HashMap<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<String, Descriptors.FileDescriptor>();

    for (DescriptorProtos.FileDescriptorProto fileProto : fileDescriptorSet.getFileList()) {
      fileDescriptorProtoMap.put(fileProto.getName(), fileDescriptorProto);
    }

    return rehydrateHelper(fileDescriptorProto, fileDescriptorProtoMap, fileDescriptorMap);
  }

  /**
   * This is the actual recursive method used by rehydrateFileDescriptor. It builds dependencies as required
   * recursively and will dynamically add the base ProtoBuf objects.
   *
   * @param fileDescriptorProto    Base FileDescriptorProto to build dependencies for.
   * @param fileDescriptorProtoMap A Map of FileDescriptorProto containing dehydrated FileDescriptorProto.
   * @param fileDescriptorMap      A Map of FileDescriptors after they have been rehydrated.
   *
   * @return A full valid FileDescriptor.
   *
   * @throws Descriptors.DescriptorValidationException
   *          Thrown if a FileDescriptorProto can not be properly converted into a FileDescriptor.
   */
  private static Descriptors.FileDescriptor rehydrateHelper(
      final DescriptorProtos.FileDescriptorProto fileDescriptorProto,
      final Map<String, DescriptorProtos.FileDescriptorProto> fileDescriptorProtoMap,
      final Map<String, Descriptors.FileDescriptor> fileDescriptorMap
  ) throws Descriptors.DescriptorValidationException
  {
    ArrayList<Descriptors.FileDescriptor> fileDependencies = new ArrayList<Descriptors.FileDescriptor>();
    for (String dependency : fileDescriptorProto.getDependencyList()) {
      if (fileDescriptorMap.containsKey(dependency)) {
        fileDependencies.add(fileDescriptorMap.get(dependency));
      } else if (fileDescriptorProtoMap.containsKey(dependency)) {
        Descriptors.FileDescriptor newFileDescriptor = rehydrateHelper(
            fileDescriptorProtoMap.get(dependency),
            fileDescriptorProtoMap,
            fileDescriptorMap
        );
        fileDescriptorProtoMap.remove(dependency);
        fileDescriptorMap.put(dependency, newFileDescriptor);
        fileDependencies.add(newFileDescriptor);
      } else if (dependency.equals("descriptor.proto")) {
        fileDependencies.add(DescriptorProtos.getDescriptor());
      } else {
        throw new IllegalStateException(String.format("Missing dependency for [%s]", dependency));
      }
    }

    return Descriptors.FileDescriptor.buildFrom(
        fileDescriptorProto,
        fileDependencies.toArray(new Descriptors.FileDescriptor[fileDependencies.size()])
    );
  }


  /* ** Construction ** */

  private MilanoTool(MilanoTypeMetadata.TypeMetadata typeMetadata)
  {
    if (typeMetadata == null) {
      throw new IllegalStateException("Received null TypeMetadata");
    }
    this.typeMetadata = typeMetadata;

    initializeDescriptors();
    log.debug("Created with TypeMetadata");
  }

  private MilanoTool(String base64)
  {
    if (base64 == null) {
      throw new IllegalStateException("Received null base64 data");
    }
    this.base64 = base64;
    try {
      this.typeMetadata = MilanoTypeMetadata.TypeMetadata
          .parseFrom(Base64.decodeBase64(StringUtils.getBytesUtf8(this.base64)));
    }
    catch (InvalidProtocolBufferException e) {
      throw Throwables.propagate(e);
    }

    initializeDescriptors();
    log.debug("Created with Base64 String");
  }

  /**
   * The builds the internal FileDescriptor and Descriptor so they don't have to be computed later.
   */
  private void initializeDescriptors()
  {
    try {
      fileDescriptor = rehydrateFileDescriptor(
          typeMetadata.getTypeFileDescriptor(),
          typeMetadata.getTypeDependencies()
      );
    }
    catch (Descriptors.DescriptorValidationException e) {
      throw Throwables.propagate(e);
    }
    descriptor = fileDescriptor.findMessageTypeByName(typeMetadata.getTypeName());

//    log.debug(String.format("Created tool with metadata:\n%s", typeMetadata));
  }

  /**
   * Build a MilanoTool from a TypeMetadata object.
   *
   * @param typeMetadata The TypeMetadata to use.
   *
   * @return A MilanoTool representing typeMetadata.
   */
  public static MilanoTool with(MilanoTypeMetadata.TypeMetadata typeMetadata)
  {
    return new MilanoTool(typeMetadata);
  }

  /**
   * Build a MilanoTool from a Base64 encoded string for use in cases where actual bytes may not be handled correctly.
   *
   * @param base64String A Base64 encoded string representing a TypeMetadata object.
   *
   * @return A Base64 Encoded String.
   */
  public static MilanoTool withBase64(String base64String)
  {
    return new MilanoTool(base64String);
  }

  /* ** Getters ** */

  /**
   * Get the Base64 representation of this MilanoTool.
   *
   * @return A Base64 encoded string.
   */
  public String getBase64()
  {
    if (base64 == null) {
      base64 = StringUtils.newStringUtf8(Base64.encodeBase64(typeMetadata.toByteArray()));
    }

    return base64;
  }

  /**
   * Get the TypeMetadata object from this MilanoTool.
   *
   * @return A TypeMetadata to be used externally.
   */
  public MilanoTypeMetadata.TypeMetadata getMetadata()
  {
    return typeMetadata;
  }

  /**
   * Get the Descriptor object from this MilanoTool.
   *
   * @return A Descriptor to encode or decode ProtoBuf Messages.
   */
  public Descriptors.Descriptor getDescriptor()
  {
    return descriptor;
  }

  /**
   * Get the DescriptorProto object from this MilanoTool. The use of this function is strongly discouraged as the
   * DescriptorProto does not contain all the necessary information to rebuild the TypeMetadata.
   *
   * @return A DescriptorProto representing the message contained.
   */
  public DescriptorProtos.DescriptorProto getDescriptorProto()
  {
    return descriptor.toProto();
  }

  /**
   * Get the FileDescriptor object from this MilanoTool.
   *
   * @return The FileDescriptor for use externally.
   */
  public Descriptors.FileDescriptor getFileDescriptor()
  {
    return fileDescriptor;
  }

  /* ** Dynamic Descriptor Helpers ** */

  public DynamicMessage.Builder newBuilder()
  {
    return DynamicMessage.newBuilder(getDescriptor());
  }

  public DynamicMessage parseFrom(InputStream input) throws IOException
  {
    return DynamicMessage.parseFrom(getDescriptor(), input);
  }

  public DynamicMessage parseFrom(InputStream input, ExtensionRegistry extensionRegistry) throws IOException
  {
    return DynamicMessage.parseFrom(getDescriptor(), input, extensionRegistry);
  }

  public DynamicMessage parseFrom(CodedInputStream input) throws IOException
  {
    return DynamicMessage.parseFrom(getDescriptor(), input);
  }

  public DynamicMessage parseFrom(CodedInputStream input, ExtensionRegistry extensionRegistry) throws IOException
  {
    return DynamicMessage.parseFrom(getDescriptor(), input, extensionRegistry);
  }

  public DynamicMessage parseFrom(ByteString data) throws InvalidProtocolBufferException
  {
    return DynamicMessage.parseFrom(getDescriptor(), data);
  }

  public DynamicMessage parseFrom(ByteString data, ExtensionRegistry extensionRegistry)
      throws InvalidProtocolBufferException
  {
    return DynamicMessage.parseFrom(getDescriptor(), data, extensionRegistry);
  }

  public DynamicMessage parseFrom(byte[] data) throws InvalidProtocolBufferException
  {
    return DynamicMessage.parseFrom(getDescriptor(), data);
  }

  public DynamicMessage parseFrom(byte[] data, ExtensionRegistry extensionRegistry)
      throws InvalidProtocolBufferException
  {
    return DynamicMessage.parseFrom(getDescriptor(), data, extensionRegistry);
  }
}
