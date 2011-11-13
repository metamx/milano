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

import com.google.common.io.LimitInputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.metamx.milano.generated.io.MilanoTypeMetadata;
import com.metamx.milano.proto.MilanoTool;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * MilanoProtoFile implements a file format for storing proto objects in a dynamically retrievable way.
 */
public final class MilanoProtoFile
{
  /**
   * Non-instantiable class.
   */
  private MilanoProtoFile()
  {}

  // The version number of the file format.
  private static byte FILE_FORMAT_VERSION = 1;

  /**
   * A reader for an MilanoProtoFile.
   */
  public static class Reader implements Closeable
  {
    private LimitInputStream inputStream;
    private CodedInputStream cis;
    private Message.Builder builder;
    private ExtensionRegistry extensionRegistry;
    private MilanoTypeMetadata.TypeMetadata typeMetadata;
    private byte fileVersion;
    private long readLimit;

    /**
     * Constructs a MilanoProtoFile.Reader
     *
     * @param inputStream       An InputStream to read messages from.
     * @param builder           A possibly null {@link com.google.protobuf.Message.Builder} representing the messages to be read. If this is null the metadata is used to build one.
     * @param extensionRegistry A possibly null {@link ExtensionRegistry}. If this is null an empty one is used.
     * @param readLimit         The maximum number of bytes to read from inputStream.
     *
     * @throws IOException Thrown on problems with inputStream.
     */
    private Reader(
        InputStream inputStream,
        Message.Builder builder,
        ExtensionRegistry extensionRegistry,
        Long readLimit
    )
        throws IOException
    {
      this.readLimit = (readLimit != null) ? readLimit : Long.MAX_VALUE;
      this.inputStream = new LimitInputStream(inputStream, this.readLimit);
      this.cis = CodedInputStream.newInstance(inputStream);
      this.fileVersion = this.cis.readRawByte();

      if (this.fileVersion != FILE_FORMAT_VERSION) {
        throw new UnsupportedEncodingException("Version [" + fileVersion + "] of MilanoProtoFile not supported.");
      }

      if (cis.readBool()) {
        // Create a new builder from the serialized Descriptor.
        MilanoTypeMetadata.TypeMetadata.Builder tmBuilder = MilanoTypeMetadata.TypeMetadata.newBuilder();
        cis.readMessage(tmBuilder, ExtensionRegistry.getEmptyRegistry());
        this.typeMetadata = tmBuilder.build();
      }

      if (builder != null) {
        this.builder = builder;
      } else if (this.typeMetadata != null) {
        // Create a new builder from the serialized Descriptor.
        this.builder = MilanoTool.with(typeMetadata).newBuilder();
      } else {
        throw new IllegalStateException("A descriptor is required for this file.");
      }

      if (extensionRegistry != null) {
        this.extensionRegistry = extensionRegistry;
      } else {
        this.extensionRegistry = ExtensionRegistry.getEmptyRegistry();
      }
    }

    public int getBytesRead()
    {
      return cis.getTotalBytesRead();
    }

    public long getBytesLeft() throws IOException
    {
      return readLimit - getBytesRead();
    }

    /**
     * Reads a Message from inputStream of the type provided by either builder, descriptor, or metadata.
     *
     * @return A {@link Message} of the type provided.
     *
     * @throws IOException Thrown for errors with inputStream.
     */
    public Message read() throws IOException
    {
      Message.Builder tempBuilder = builder.clone();
      cis.readMessage(tempBuilder, extensionRegistry);
      return tempBuilder.build();
    }

    public boolean empty() throws IOException
    {
      return cis.isAtEnd();
    }

    /**
     * This retrieves any metadata stored with the file upon creation.
     * It is purely specified by the end user and has no internal purpose.
     *
     * @return An immutable, possibly null, Map of String to ByteString contained in the metadata.
     */
    public MilanoTypeMetadata.TypeMetadata getMetadata()
    {
      if (typeMetadata == null) {
        return null;
      }

      return typeMetadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException
    {
      inputStream.close();
    }

  }

  /**
   * A writer for an MilanoProtoFile.
   */
  public static class Writer implements Closeable, Flushable
  {

    private OutputStream outputStream;
    private CodedOutputStream cos;

    /**
     * Construct an MilanoProtoFile.Writer
     *
     * @param outputStream The {@link OutputStream} to write to.
     * @param typeMetadata A possibly null {@link MilanoTypeMetadata.TypeMetadata} containing any metadata that needs to be stored.
     *
     * @throws IOException This is thrown if there are any problems writing to the OutputStream.
     */
    private Writer(OutputStream outputStream, MilanoTypeMetadata.TypeMetadata typeMetadata) throws IOException
    {
      this.outputStream = outputStream;
      this.cos = CodedOutputStream.newInstance(outputStream);

      // Set the version for this file.
      cos.writeRawByte(FILE_FORMAT_VERSION);

      if (typeMetadata == null) {
        cos.writeBoolNoTag(false);
      } else {
        cos.writeBoolNoTag(true);
        write(typeMetadata);
      }
    }

    /**
     * Writes a {@link Message} to the provided outputStream.
     *
     * @param message A message to write to the stream. This should be of the type passed in the metadata,
     *                or in the case that no metadata was passed should be the same as all other messages.
     *
     * @throws IOException This is thrown on any problem with the OutputStream.
     */
    public void write(Message message) throws IOException
    {
      // TODO: Do we want to validate that message is of the same type as the passed descriptor/typeMetadata?
      cos.writeMessageNoTag(message);
    }

    @Override
    public void flush() throws IOException
    {
      cos.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException
    {
      cos.flush();
      outputStream.flush();
      outputStream.close();
    }
  }


  public static Reader createReader(InputStream inputStream) throws IOException
  {
    return createReader(inputStream, (Message.Builder) null);
  }

  public static Reader createReader(InputStream inputStream, Message.Builder builder) throws IOException
  {
    return createReader(inputStream, builder, (ExtensionRegistry) null);
  }

  public static Reader createReader(InputStream inputStream, ExtensionRegistry extensionRegistry) throws IOException
  {
    return createReader(inputStream, (Message.Builder) null, extensionRegistry);
  }

  public static Reader createReader(
      InputStream inputStream,
      Message.Builder builder,
      ExtensionRegistry extensionRegistry
  ) throws IOException
  {
    return createReader(inputStream, builder, extensionRegistry, (Long) null);
  }

  /**
   * Create a Reader.
   *
   * @param inputStream       The InputStream to read from.
   * @param builder           A {@link Message.Builder} of the type to read from the file. If this is null it
   *                          will use the metadata to decode the file. If this is null and there is no metadata
   *                          present an IllegalStateException will be thrown.
   * @param extensionRegistry An {@link ExtensionRegistry} which may be required to decode types from the file.
   *                          This may be null, in which case the metadata will supply this information. It is
   *                          also possible that this is not encoded in the metadata in which case the messages
   *                          will be decoded by builder (or the metadata equivalent) and the fields within the
   *                          ExtensionRegistry will be left out and placed in unknown fields.
   * @param readLimit         The maximum number of bytes to read from inputStream.
   *
   * @return A Reader for an MilanoProtoFile.
   *
   * @throws IOException Thrown when an error occurs with the inputStream.
   */
  public static Reader createReader(
      InputStream inputStream,
      Message.Builder builder,
      ExtensionRegistry extensionRegistry,
      Long readLimit
  ) throws IOException
  {
    return new Reader(inputStream, builder, extensionRegistry, readLimit);
  }

  public static Writer createWriter(OutputStream outputStream) throws IOException
  {
    return createWriter(outputStream, (MilanoTypeMetadata.TypeMetadata) null);
  }

  /**
   * Create a Writer.
   *
   * @param outputStream An OutputStream to write to.
   * @param typeMetadata The {@link com.metamx.milano.generated.io.MilanoTypeMetadata.TypeMetadata} encoding the {@link Message} type and any
   *                     additional metadata information. If this is null no metadata will be stored in which
   *                     case the file will not be able to be dynamically decoded.
   *
   * @return A Writer for an MilanoProtoFile.
   *
   * @throws IOException Thrown whan an error occurs with the outputStream.
   */
  public static Writer createWriter(OutputStream outputStream, MilanoTypeMetadata.TypeMetadata typeMetadata)
      throws IOException
  {
    return new Writer(outputStream, typeMetadata);
  }
}
