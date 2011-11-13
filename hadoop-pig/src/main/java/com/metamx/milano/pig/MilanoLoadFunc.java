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
package com.metamx.milano.pig;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.metamx.milano.generated.io.MilanoTypeMetadata;
import com.metamx.milano.hadoop.MilanoProtoFileInputFormat;
import com.metamx.milano.hadoop.MilanoProtoFileRecordReader;
import com.metamx.milano.io.MilanoProtoFile;
import com.metamx.milano.proto.MilanoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 */
public class MilanoLoadFunc extends LoadFunc implements LoadMetadata //, LoadPushDown -- Not yet supported
{
  private static final Logger log = Logger.getLogger(MilanoLoadFunc.class);
  private String udfSignature;
  private MilanoProtoFileRecordReader recordReader;
  private TupleFactory tupleFactory = TupleFactory.getInstance();
  private BagFactory bagFactory = BagFactory.getInstance();
  private MilanoTypeMetadata.TypeMetadata typeMetadata;
  private Descriptors.Descriptor descriptor;


  /**
   * Set the UDF Signature which is used to store the ProtoBuf Schema between the client and mapper/reducer.
   * Called in both the client and mapper/reducer context.
   *
   * @param signature The UDF signature.
   */
  @Override
  public void setUDFContextSignature(String signature)
  {
    udfSignature = signature;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException
  {
    Path basePath = new Path(location);
    FileSystem fileSystem = basePath.getFileSystem(job.getConfiguration());

    Set<Path> paths = new TreeSet<Path>();

    if (fileSystem.getFileStatus(basePath).isDir()) {
      getPaths(basePath, paths, fileSystem);
    } else {
      paths.add(basePath);
    }

    log.info("Setting input to " + paths);
    FileInputFormat.setInputPaths(job, Joiner.on(',').join(paths));
  }

  private void getPaths(Path baseDirectory, Set<Path> paths, FileSystem fileSystem) throws IOException
  {
    FileStatus[] files = fileSystem.listStatus(baseDirectory);
    for (FileStatus file : files) {
      Path path = file.getPath();
      FileStatus fileStatus = fileSystem.getFileStatus(path);
      if (fileStatus.isDir()) {
        getPaths(path, paths, fileSystem);
      } else {
        paths.add(baseDirectory);
      }
    }
  }

  @Override
  public InputFormat getInputFormat() throws IOException
  {
    log.debug("Getting InputFormat");
    return new MilanoProtoFileInputFormat();
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException
  {
    log.debug("Preparing to read");
    recordReader = (MilanoProtoFileRecordReader) reader;
    typeMetadata = recordReader.getMetadata();
    descriptor = MilanoTool.with(typeMetadata).getDescriptor();
  }

  @Override
  public Tuple getNext() throws IOException
  {
    try {
      if (!recordReader.nextKeyValue()) {
        return null;
      }

      return buildTuple(recordReader.getCurrentValue(), descriptor);
    }
    catch (InterruptedException e) {
      log.error("Interrupted", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * This recursively builds a Pig Tuple from a Message and a Descriptor.
   *
   * @param message    The Message to decode into a Tuple.
   * @param descriptor The Descriptor to use in decoding the Message.
   *
   * @return The new Tuple.
   *
   * @throws IOException Thrown when we receive an unsupported type in the Message/Descriptor (ENUM/BOOLEAN).
   */
  private Tuple buildTuple(Message message, final Descriptors.Descriptor descriptor) throws IOException
  {
    List<Object> tuple = new ArrayList<Object>();

    for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
      // HACK: For some reason the FieldDescriptor from the Descriptor doesn't match the one from the Message.
      // HACK: This is a hack to get around that problem.
      Descriptors.FieldDescriptor messageFieldDescriptor = message.getDescriptorForType()
                                                                  .findFieldByName(fieldDescriptor.getName());
      switch (fieldDescriptor.getJavaType()) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case STRING:
          tuple.add(message.getField(messageFieldDescriptor));
          break;
        case BYTE_STRING:
          // Pig doesn't understand ByteString. Here we convert to a byte[] which Pig does understand.
          tuple.add(new DataByteArray(((ByteString) message.getField(messageFieldDescriptor)).toByteArray()));
          break;

        // This functionality is totally untested.
        case MESSAGE:
          if (fieldDescriptor.isRepeated()) {
            // We have a bag.
            List<Tuple> bag = new ArrayList<Tuple>();

            int count = message.getRepeatedFieldCount(messageFieldDescriptor);
            for (int i = 0; i < count; i++) {
              bag.add(
                  buildTuple(
                      (Message) message.getRepeatedField(messageFieldDescriptor, i),
                      fieldDescriptor.getMessageType()
                  )
              );
            }

            tuple.add(bagFactory.newDefaultBag(bag));
          } else {
            // Just a tuple.
            tuple.add(
                buildTuple(
                    (Message) message.getField(messageFieldDescriptor),
                    messageFieldDescriptor.getMessageType()
                )
            );
          }
          break;

        case ENUM:
        case BOOLEAN:
          throw new IOException(String.format("Type %s not supported.", fieldDescriptor.getJavaType().toString()));
      }
    }

    return tupleFactory.newTuple(tuple);
  }

  /**
   * This builds a Pig ResourceSchema from the input file(s). This relies on the existence of TypeMetadata.
   * This is the method by which we pass the schema types and names directly to pig without having to specify them directly.
   *
   * @param location As passed to relativeToAbsolutePath
   * @param job      The job.
   *
   * @return Returns a ResourceSchema representing the incoming file(s) or null if TypeMetadata does not exist.
   *
   * @throws IOException Not thrown directly, but thrown from getMessageSchema where it indicates an unsupported type.
   */
  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException
  {
    Configuration conf = job.getConfiguration();
    Properties props = ConfigurationUtil.toProperties(conf);

    // HACK: Here we open the file directly to read the TypeMetadata.
    // HACK: There may be a better more direct way to do this, but it works for now.
    Path path = new Path(location);
    FileSystem fileSystem = path.getFileSystem(conf);

    FileStatus fileStatus = fileSystem.getFileStatus(path);
    if (fileStatus.isDir()) {
      log.debug(String.format("Path is a directory."));
      path = getFilePath(path, fileSystem);
      if (path == null) {
        return null;
      }
    } else if (!fileSystem.exists(path)) {
      return null;
    }

    MilanoProtoFile.Reader reader = MilanoProtoFile.createReader(fileSystem.open(path));
    typeMetadata = reader.getMetadata();
    reader.close();

    if (typeMetadata == null) {
      return null;
    }
    descriptor = MilanoTool.with(typeMetadata).getDescriptor();

    return new ResourceSchema(getMessageSchema(descriptor));
  }

  private Path getFilePath(Path path, FileSystem fileSystem) throws IOException
  {
    Path newPath = null;
    FileStatus[] files = fileSystem.listStatus(path);
    for (FileStatus file : files) {
      if (file.isDir()) {
        newPath = getFilePath(file.getPath(), fileSystem);
        if (newPath != null) {
          break;
        }
      } else {
        newPath = file.getPath();
        break;
      }
    }

    return newPath;
  }

  /**
   * This takes a Descriptor and recursively creates a Pig Schema out of it.
   *
   * @param descriptor The descriptor to use.
   *
   * @return A Schema representing the structure of the descriptor.
   *
   * @throws IOException Thrown if an unsupported type is encountered.
   */
  private Schema getMessageSchema(final Descriptors.Descriptor descriptor) throws IOException
  {
    Schema schema = new Schema();
    for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
      String name = fieldDescriptor.getName();
      switch (fieldDescriptor.getJavaType()) {
        case INT:
          schema.add(new Schema.FieldSchema(name, DataType.INTEGER));
          break;
        case LONG:
          schema.add(new Schema.FieldSchema(name, DataType.LONG));
          break;
        case FLOAT:
          schema.add(new Schema.FieldSchema(name, DataType.FLOAT));
          break;
        case DOUBLE:
          schema.add(new Schema.FieldSchema(name, DataType.DOUBLE));
          break;
        case STRING:
          schema.add(new Schema.FieldSchema(name, DataType.CHARARRAY));
          break;
        case BYTE_STRING:
          schema.add(new Schema.FieldSchema(name, DataType.BYTEARRAY));
          break;
        case MESSAGE:
          Descriptors.Descriptor messageType = fieldDescriptor.getMessageType();
          if (fieldDescriptor.isRepeated()) {
            // We have a bag.
            schema.add(new Schema.FieldSchema(name, getMessageSchema(messageType), DataType.BAG));
          } else {
            // Just a tuple.
            schema.add(new Schema.FieldSchema(name, getMessageSchema(messageType), DataType.TUPLE));
          }
          break;
        case ENUM:
        case BOOLEAN:
        default:
          throw new IOException("Unsupported data type.");
      }
    }

    return schema;
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException
  {
    return null; //Not supported.
  }

  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException
  {
    return null; //Not supported.
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException
  {
    // No-Op not supported.
  }
/*  // Not yet supported, scheduled for later release.
  @Override
  public List<LoadPushDown.OperatorSet> getFeatures()
  {
    return null;
  }

  @Override
  public LoadPushDown.RequiredFieldResponse pushProjection(LoadPushDown.RequiredFieldList requiredFieldList)
      throws FrontendException
  {
    return null;
  }
*/
}
