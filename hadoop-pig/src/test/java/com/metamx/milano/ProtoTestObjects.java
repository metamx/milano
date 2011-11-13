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
package com.metamx.milano;

import com.google.common.base.Throwables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.metamx.milano.proto.MilanoTool;
import com.metamx.milano.proto.Testing;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProtoTestObjects
{
  FileSystem fs = null;
  TaskAttemptContext context = null;
  Path workingPath = null;

  List<Map<String, Object>> testHashes = new ArrayList<Map<String, Object>>();
  List<Testing.TestItem> testItems = new ArrayList<Testing.TestItem>();
  List<byte[]> testItemBytes = new ArrayList<byte[]>();

  public ProtoTestObjects()
  {
    Configuration conf = new Configuration(true);
    try {
      fs = FileSystem.getLocal(conf);
    }
    catch (IOException e) {
      IOUtils.closeQuietly(fs);
      throw Throwables.propagate(e);
    }

    workingPath = new Path(fs.getWorkingDirectory(), "test-temporary-files");
    try {
      fs.mkdirs(workingPath);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    conf.set("mapred.input.dir", workingPath.toString());
    conf.set("mapred.output.dir", workingPath.toString());
    conf.set("mapred.output.key.class", Map.class.getName());
    conf.set("mapred.output.value.class", Text.class.getName());

    conf.set("milano.proto.builder", Testing.TestItem.class.getName());

    context = new TaskAttemptContext(conf, new TaskAttemptID());

    HashMap<String, Object> testHash0 = new HashMap<String, Object>();

    testHash0.put("id", 1);
    testHash0.put("name", "Test 1");
    testHash0.put("type", "FOO");
    testHash0.put("data", Arrays.asList("Datum 1", "Datum 2"));

    HashMap<String, Object> itemMap0 = new HashMap<String, Object>();
    itemMap0.put("id", 42);
    itemMap0.put("name", "Foo");
    testHash0.put("item", itemMap0);

    Testing.TestItem testItem0 = Testing.TestItem.newBuilder()
                                                 .setId(1)
                                                 .setName("Test 1")
                                                 .setType(Testing.TestItem.Type.FOO)
                                                 .addData("Datum 1")
                                                 .addData("Datum 2")
                                                 .setItem(
                                                     Testing.TestItem
                                                         .SubItem
                                                         .newBuilder()
                                                         .setId(42)
                                                         .setName("Foo")
                                                         .build()
                                                 )
                                                 .build();

    Assert.assertTrue(testItem0.isInitialized());

    byte[] testItemBytes0 = testItem0.toByteArray();
    Testing.TestItem newItem0 = null;
    try {
      newItem0 = Testing.TestItem.parseFrom(testItemBytes0);
    }
    catch (InvalidProtocolBufferException e) {
      throw Throwables.propagate(e);
    }

    Assert.assertEquals(testItem0, newItem0);

    testItems.add(0, testItem0);
    testHashes.add(0, testHash0);
    testItemBytes.add(0, testItemBytes0);


    HashMap<String, Object> testHash1 = new HashMap<String, Object>();

    testHash1.put("id", 2);
    testHash1.put("name", "Test 2");
    testHash1.put("type", "BAR");
    testHash1.put("data", Arrays.asList("Datum 3", "Datum 4"));

    HashMap<String, Object> itemMap1 = new HashMap<String, Object>();
    itemMap1.put("id", 24);
    itemMap1.put("name", "Bar");
    testHash1.put("item", itemMap1);

    Testing.TestItem testItem1 =
        Testing.TestItem.newBuilder()
                        .setId(2)
                        .setName("Test 2")
                        .setType(Testing.TestItem.Type.BAR)
                        .addData("Datum 3")
                        .addData("Datum 4")
                        .setItem(
                            Testing.TestItem
                                .SubItem
                                .newBuilder()
                                .setId(24)
                                .setName("Bar")
                                .build()
                        )
                        .build();

    Assert.assertTrue(testItem1.isInitialized());

    byte[] testItemBytes1 = testItem1.toByteArray();
    Testing.TestItem newItem1 = null;
    try {
      newItem1 = Testing.TestItem.parseFrom(testItemBytes1);
    }
    catch (InvalidProtocolBufferException e) {
      throw Throwables.propagate(e);
    }

    Assert.assertEquals(testItem1, newItem1);

    testItems.add(1, testItem1);
    testHashes.add(1, testHash1);
    testItemBytes.add(1, testItemBytes1);


    HashMap<String, Object> testHash2 = new HashMap<String, Object>();

    testHash2.put("id", 2);
    testHash2.put("name", "Test 3");
    testHash2.put("type", "BAZ");
    testHash2.put("data", Arrays.asList("Datum 5", "Datum 6"));

    HashMap<String, Object> itemMap2 = new HashMap<String, Object>();
    itemMap2.put("id", 37);
    itemMap2.put("name", "Baz");
    testHash2.put("item", itemMap2);

    Testing.TestItem testItem2 =
        Testing.TestItem.newBuilder()
                        .setId(2)
                        .setName("Test 3")
                        .setType(Testing.TestItem.Type.BAZ)
                        .addData("Datum 5")
                        .addData("Datum 6")
                        .setItem(
                            Testing.TestItem
                                .SubItem
                                .newBuilder()
                                .setId(37)
                                .setName("Baz")
                                .build()
                        )
                        .build();


    Assert.assertTrue(testItem2.isInitialized());

    byte[] testItemBytes2 = testItem2.toByteArray();
    Testing.TestItem newItem2 = null;
    try {
      newItem2 = Testing.TestItem.parseFrom(testItemBytes2);
    }
    catch (InvalidProtocolBufferException e) {
      throw Throwables.propagate(e);
    }

    Assert.assertEquals(testItem2, newItem2);

    testItems.add(2, testItem2);
    testHashes.add(2, testHash2);
    testItemBytes.add(2, testItemBytes2);
  }

  public TaskAttemptContext getContext()
  {
    return context;
  }

  public Map<String, Object> getTestMap(int i)
  {
    return testHashes.get(i);
  }

  public Testing.TestItem getTestItem(int i)
  {
    return testItems.get(i);
  }

  public byte[] getTestItemBytes(int i)
  {
    return testItemBytes.get(i);
  }

  public FileSystem getFs()
  {
    return fs;
  }

  public Path getWorkingPath()
  {
    return workingPath;
  }

  public List<Map<String, Object>> getTestMaps()
  {
    return testHashes;
  }

  public List<Testing.TestItem> getTestItems()
  {
    return testItems;
  }

  public List<byte[]> getTestItemBytes()
  {
    return testItemBytes;
  }

  public Descriptors.Descriptor getDynamicDescriptor()
      throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException
  {
    String wireType = MilanoTool.with(
            Testing.TestItem
                .getDescriptor()
                .getName(),
            Testing.getDescriptor()
    ).getBase64();

    return MilanoTool.withBase64(wireType).getDescriptor();
  }

  public void compareMessages(Message expected, Message actual)
  {
    Map<Descriptors.FieldDescriptor, Object> expectedFields = expected.getAllFields();
    Map<Descriptors.FieldDescriptor, Object> actualFields = actual.getAllFields();

    Descriptors.Descriptor expectedDescriptor = expected.getDescriptorForType();

    Set<Descriptors.FieldDescriptor> actualKeys = actualFields.keySet();

    for (Descriptors.FieldDescriptor i : actualKeys) {

      switch (i.getJavaType()) {
        case ENUM: {
          break;
        }
        case MESSAGE: {
          Message expectedMsg = (Message) expectedFields.get(expectedDescriptor.findFieldByName(i.getName()));
          Message actualMsg = (Message) actualFields.get(i);

          compareMessages(expectedMsg, actualMsg);
          break;
        }
        default: {
          Assert.assertEquals(
              expectedFields.get(expectedDescriptor.findFieldByName(i.getName())),
              actualFields.get(i)
          );
        }
      }
    }
  }

  public void compareMaps(Map<String, Object> expected, Map<String, Object> actual)
  {
    Set<String> expectedKeys = expected.keySet();
    Set<String> actualKeys = actual.keySet();

    Assert.assertEquals("Key sets do not match.", expectedKeys, actualKeys);

    for (String key : expectedKeys) {
      Object expectedValue = expected.get(key);
      Object actualValue = actual.get(key);

      if (expectedValue instanceof List) {
        @SuppressWarnings("unchecked")
        List<Object> expectedList = (List<Object>) expectedValue;
        @SuppressWarnings("unchecked")
        List<Object> actualList = (List<Object>) actualValue;

        Assert.assertEquals(expectedList.size(), actualList.size());

        if (expectedList.get(0) instanceof Map) {
          for (int i = 0; i < expectedList.size(); i++) {
            @SuppressWarnings("unchecked")
            Map<String, Object> expectedMap = (Map<String, Object>) expectedList.get(i);
            @SuppressWarnings("unchecked")
            Map<String, Object> actualMap = (Map<String, Object>) actualList.get(i);

            Assert.assertEquals("Different size maps.", expectedMap.size(), actualMap.size());

            compareMaps(expectedMap, actualMap);
          }
        } else {
          Assert.assertArrayEquals(expectedList.toArray(), actualList.toArray());
        }
      } else if (expectedValue instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> expectedMap = (Map<String, Object>) expectedValue;
        @SuppressWarnings("unchecked")
        Map<String, Object> actualMap = (Map<String, Object>) actualValue;

        Assert.assertEquals("Different Size Maps", expectedMap.size(), actualMap.size());

        compareMaps(expectedMap, actualMap);
      } else {
        Assert.assertEquals("Different values for key: " + key, expectedValue, actualValue);
      }
    }
  }


  public List<Descriptors.Descriptor> getDescriptors(Descriptors.Descriptor descriptor)
  {
    List<Descriptors.Descriptor> list = new ArrayList<Descriptors.Descriptor>();
    list.add(descriptor);

    for (Descriptors.Descriptor desc : descriptor.getNestedTypes()) {
      list.addAll(getDescriptors(desc));
    }

    return list;
  }
}
