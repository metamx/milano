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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.metamx.milano.io.MilanoProtoFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;
import java.util.Map;

/**
 *
 */
public class MilanoStoreLoadFuncTests
{
  private static final Logger log = Logger.getLogger(MilanoStoreLoadFuncTests.class);

//  @Ignore("Incomplete test")
  @Test
  public void testDecodeFile() throws Exception
  {
    Path path = new Path("/tmp/proto-test-2/y=2011/m=08/d=30/H=23/task_local_0014_r_000000");
    FSDataInputStream inputStream = path.getFileSystem(new Configuration()).open(path, 6000000);

//    FileInputStream fileInputStream = new FileInputStream(
//        "/tmp/proto-test-2/y=2011/m=08/d=30/H=00/task_local_0014_r_000000"
//    );

    MilanoProtoFile.Reader reader = MilanoProtoFile.createReader(inputStream);

    while(!reader.empty()) {
      Message message = reader.read();

      Map<Descriptors.FieldDescriptor, Object> fields = message.getAllFields();
      for (Object o : fields.values()) {
        System.out.print(o.toString() + ",");
      }
      System.out.println();
    }
  }
}
