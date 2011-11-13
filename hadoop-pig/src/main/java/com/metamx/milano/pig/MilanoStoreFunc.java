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

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.metamx.milano.hadoop.MilanoProtoFileOutputFormat;
import com.metamx.milano.proto.MilanoTool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

/**
 *
 */
public class MilanoStoreFunc extends StoreFunc
{
  private Logger log = Logger.getLogger(MilanoStoreFunc.class);

  private RecordWriter<String, Message> recordWriter;
  private String udfSignature;
  private Descriptors.Descriptor schemaDescriptor;


  /**
   * Set the UDF Signature which is used to store the ProtoBuf Schema between the client and mapper/reducer.
   * Called in both the client and mapper/reducer context.
   *
   * @param signature The UDF signature.
   */
  @Override
  public void setStoreFuncUDFContextSignature(String signature)
  {
    udfSignature = signature;
  }

  /* *** Client side calls *** */

  /**
   * Here we take the ResourceSchema and generate the ProtoBuf Descriptor.
   * We take the Descriptor, base64 encode it, and store it into the UDFContext.
   * The majority of the work of this function is done in getProtoSchema
   *
   * @param schema The ResourceSchema supplied by Pig.
   *
   * @throws IOException Thrown if the descriptor can not be validated.
   */
  @Override
  public void checkSchema(ResourceSchema schema) throws IOException
  {
    Properties props = getUDFProps();

    log.debug(String.format("Generating Descriptor and storing base64 into \"milano.pig.proto.schema.base64\""));
    DescriptorProtos.DescriptorProto protoSchema = getProtoSchema(schema);

    DescriptorProtos.FileDescriptorProto fileDescriptorProto = DescriptorProtos.FileDescriptorProto
        .newBuilder()
        .addMessageType(protoSchema)
//        .setName("milano_dynamic_type.proto")
//        .setPackage("metamx.milano.dynamic")
        .build();

    props.setProperty(
        "milano.pig.proto.schema.base64",
        MilanoTool.with(
            protoSchema,
            fileDescriptorProto,
            DescriptorProtos.FileDescriptorSet.getDefaultInstance()
        ).getBase64()
    );
  }

  /**
   * This takes a Pig ResourceSchema and build a ProtoBuf DescriptorProto out of it.
   *
   * @param schema The ResourceSchema to create a DescriptorProto for.
   *
   * @return A DescriptorProto matching the ResourceSchema. Note that this is not a Descriptor but the Proto Object representing one.
   *
   * @throws IOException Thrown if the ResourceSchema contains an unsupported or unknown type.
   */
  private DescriptorProtos.DescriptorProto getProtoSchema(ResourceSchema schema) throws IOException
  {
    DescriptorProtos.DescriptorProto.Builder builder = DescriptorProtos.DescriptorProto.newBuilder();
    builder.setName("PigTupleSchema");
    int index = 1;

    for (ResourceSchema.ResourceFieldSchema fieldSchema : schema.getFields()) {
      String fieldName = fieldSchema.getName();

      fieldName = fieldName.substring(fieldName.lastIndexOf(':') + 1);

      log.debug(
          String.format(
              "Starting field [%s] of type [%s] for index [%s] with full name [%s]",
              fieldName,
              DataType.findTypeName(fieldSchema.getType()),
              index,
              fieldSchema.getName()
          )
      );

      switch (fieldSchema.getType()) {

        // String Types handled with addStringField
        case DataType.CHARARRAY:
          addField(builder, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, index, fieldName);
          break;
        case DataType.BYTEARRAY:
          addField(builder, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, index, fieldName);
          break;

        case DataType.DOUBLE:
          addField(builder, DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, index, fieldName);
          break;
        case DataType.FLOAT:
          addField(builder, DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, index, fieldName);
          break;
        case DataType.INTEGER:
          addField(builder, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, index, fieldName);
          break;
        case DataType.LONG:
          addField(builder, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, index, fieldName);
          break;

        // Containers These are totally untested.
        case DataType.TUPLE:
          // Recurse
          log.debug(String.format("Adding tuple field [%s]", fieldName));
          builder.addFieldBuilder()
                 .mergeFrom(getProtoSchema(fieldSchema.getSchema()))
                 .setName(fieldName)
                 .setNumber(index)
                 .build();
          break;
        case DataType.BAG:
          // Create list and recurse
          log.debug(String.format("Adding bag field [%s]", fieldName));
          ResourceSchema.ResourceFieldSchema[] fs = fieldSchema.getSchema().getFields();
          if (fs.length != 1 || fs[0].getType() != DataType.TUPLE) {
            throw logAndReturnIOE("Found a bag without a tuple inside!");
          }

          builder.addFieldBuilder()
                 .setOptions(builder.addFieldBuilder().getOptionsBuilder().setPacked(true))
                 .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                 .mergeFrom(getProtoSchema(fieldSchema.getSchema()))
                 .setName(fieldName)
                 .setNumber(index)
                 .build();
          break;

        // Unsupported Types
        case DataType.MAP:
          throw logAndReturnIOE("The Pig MAP type is currently not supported.");
        case DataType.NULL:
          throw logAndReturnIOE("The Pig NULL type is currently not supported.");
        case DataType.ERROR:
          throw logAndReturnIOE("The Pig ERROR type is currently not supported.");
        case DataType.BIGCHARARRAY:
        case DataType.GENERIC_WRITABLECOMPARABLE:
          throw new UnsupportedOperationException("Pig datatype not supported.");


          // Pig Internal Types
        case DataType.BOOLEAN:
        case DataType.BYTE:
        case DataType.INTERNALMAP:
          throw logAndReturnIOE("Use of internal type in schema definition.");

          // Unknown (duh)
        case DataType.UNKNOWN:
        default:
          throw logAndReturnIOE("Unknown data type.");
      }

      index++;
    }

    return builder.build();
  }

  /**
   * This adds a field to a DescriptorProto being built.
   *
   * @param builder   The builder to add the field to.
   * @param fieldType The Type of field to add.
   * @param index     The number to assign the field.
   * @param name      The name to give the field.
   */
  private void addField(
      final DescriptorProtos.DescriptorProto.Builder builder,
      final DescriptorProtos.FieldDescriptorProto.Type fieldType,
      final int index,
      final String name
  )
  {
    log.debug(String.format("Adding field [%s] of type [%s]", name, fieldType.name()));
    builder.addFieldBuilder()
           .setType(fieldType)
           .setName(name)
           .setNumber(index)
           .build();
  }

  /* *** Mapper/Reducer side calls *** */

  /**
   * This does the setup for the mapper/reducer side.
   *
   * @param location The output path.
   * @param job      The job config.
   *
   * @throws IOException Currently not thrown, but is part of the overridden signature.
   */
  @Override
  public void setStoreLocation(String location, Job job) throws IOException
  {
    FileOutputFormat.setOutputPath(job, new Path(location));
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    Properties props = getUDFProps();


    job.getConfiguration()
       .set("com.metamx.milano.proto.descriptor.base64", (String) props.get("milano.pig.proto.schema.base64"));
  }

  /**
   * This retrieves the OutputFormat for use by Hadoop.
   *
   * @return An {@link com.metamx.milano.hadoop.MilanoProtoFileOutputFormat}
   *
   * @throws IOException Currently not thrown, but part of the overridden signature.
   */
  @Override
  public OutputFormat getOutputFormat() throws IOException
  {
    assert udfSignature != null;

    log.debug("Getting OutputFormat");

    return new MilanoProtoFileOutputFormat();
  }

  /**
   * Prepare this for writing to the OutputFormat.
   * This expects "milano.pig.proto.schema.base64" to be present in the UDFContext.
   *
   * @param writer The RecordWriter supplied by the OutputFormat returned from getOutputFormat.
   *
   * @throws IOException Thrown when the UDFContext contains an invalid or non-existent base64 encoded DescriptorProto bytes.
   */
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException
  {
    log.debug("Preparing to write");

    @SuppressWarnings("unchecked")
    RecordWriter<String, Message> tempWriter = writer;
    recordWriter = tempWriter;
    Properties props = getUDFProps();
    schemaDescriptor = MilanoTool.withBase64((String) props.get("milano.pig.proto.schema.base64")).getDescriptor();
  }

  /**
   * This takes a Pig Tuple, converts it to a ProtoBuf Message, and writes it to the RecordWriter.
   * The majority of the work for this function is done by serializeTuple.
   *
   * @param tuple The Tuple to convert and write out.
   *
   * @throws IOException Thrown when interrupted or if serializeTuple throws the same.
   */
  @Override
  public void putNext(Tuple tuple) throws IOException
  {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(schemaDescriptor);
    serializeTuple(tuple, builder, schemaDescriptor);
    try {
      recordWriter.write("", builder.build());
    }
    catch (InterruptedException e) {
      throw new IOException(e);
    }

  }

  /**
   * This takes a Tuple and sets up messageBuilder.
   * The messageBuilder could be for a different type than the tupleDescriptor.
   * The tupleDescriptor is used to parse the incoming Tuples.
   * The messageBuilder is the type to actually write out to the file.
   *
   * @param tuple           The Tuple to process.
   * @param messageBuilder  A message builder for the output Message type.
   * @param tupleDescriptor A descriptor used to decode the Tuple (as generated by getProtoSchema).
   *
   * @throws IOException Thrown when a unsupported type is encountered in the tupleDescriptor.
   */
  private void serializeTuple(
      final Tuple tuple,
      final Message.Builder messageBuilder,
      final Descriptors.Descriptor tupleDescriptor
  ) throws IOException
  {
    Iterator<Object> tupleIterator = tuple.getAll().iterator();
    Descriptors.Descriptor messageDescriptor = messageBuilder.getDescriptorForType();

    // Here we process the two descriptors in parallel. For the current version this is actually unnecessary,
    // but this is in anticipation of being able to specify the actual message without using the dynamic TypeMetadata.
    for (Descriptors.FieldDescriptor tupleFieldDescriptor : tupleDescriptor.getFields()) {
      Object item = tupleIterator.next();
      Message.Builder itemBuilder;

      if (item == null) {
        continue;
      }

      Descriptors.FieldDescriptor messageFieldDescriptor = messageDescriptor.findFieldByName(tupleFieldDescriptor.getName());
      switch (tupleFieldDescriptor.getJavaType()) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case STRING:
          messageBuilder.setField(messageFieldDescriptor, item);
          break;
        case BYTE_STRING:
          // Here we convert the Pig object into bytes to be serialized, this could contain arbitrary binary data so it must be handled differently.
          messageBuilder.setField(messageFieldDescriptor, ByteString.copyFrom(DataType.toBytes(item)));
          break;

        // This functionality is totally untested.
        case MESSAGE:
          Descriptors.Descriptor subTupleDescriptor = tupleFieldDescriptor.getMessageType();
          if (tupleFieldDescriptor.isRepeated()) {
            //This is a bag
            for (Object subTuple : ((DataBag) item)) {
              itemBuilder = messageBuilder.newBuilderForField(messageFieldDescriptor);
              serializeTuple((Tuple) subTuple, itemBuilder, subTupleDescriptor);
              messageBuilder.addRepeatedField(messageFieldDescriptor, itemBuilder);
            }
          } else {
            //This is a tuple
            itemBuilder = messageBuilder.newBuilderForField(messageFieldDescriptor);
            serializeTuple((Tuple) item, itemBuilder, subTupleDescriptor);
            messageBuilder.setField(tupleFieldDescriptor, itemBuilder);
          }
          break;

        case BOOLEAN:
          log.error("Boolean field in generated tuple descriptor");
          throw logAndReturnIOE("Boolean field in generated tuple descriptor.");
        case ENUM:
          log.error("Enum field in generated tuple descriptor");
          throw logAndReturnIOE("Enum field in generated tuple descriptor.");
        default:
          throw logAndReturnIOE("Unknown data type.");
      }
    }
  }

  /**
   * Helper function to log and then return an IOE.
   *
   * @param message The message to report to both the logger and set in the IOE.
   *
   * @return An IOE with the given message.
   */
  private IOException logAndReturnIOE(String message)
  {
    IOException ioe = new IOException(message);
    log.error(message, ioe);
    return ioe;
  }

  /**
   * Retrieves the Properties object from the UDFContext.
   *
   * @return The properties for this UDF.
   */
  private Properties getUDFProps()
  {
    UDFContext udfContext = UDFContext.getUDFContext();
    return udfContext.getUDFProperties(this.getClass(), new String[]{udfSignature});
  }
}
