/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package phoenix.datatorrent.operator.input.kafka;

import java.nio.ByteBuffer;

import kafka.message.Message;

import org.apache.hadoop.io.BytesWritable;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.kafka.AbstractPartitionableKafkaInputOperator;

/**
 * This class just emit one constant msg for each kafka msg received So we can track the throughput
 * by msgs emitted per second in the stram platform
 *
 * @since 0.9.3
 */
public class PartitionableKafkaInputOperator extends AbstractPartitionableKafkaInputOperator {

  public transient DefaultOutputPort<byte[]> oport = new DefaultOutputPort<byte[]>();

  @Override
  protected AbstractPartitionableKafkaInputOperator cloneOperator() {
    return new PartitionableKafkaInputOperator();
  }

  private BytesWritable getBytesFromKafka(Message msg) {
    ByteBuffer buf = msg.payload();
    BytesWritable payload = new BytesWritable();
    int origSize = buf.remaining();
    byte[] bytes = new byte[origSize];
    buf.get(bytes, buf.position(), origSize);
    payload.set(bytes, 0, origSize);
    return payload;
  }

  @Override
  protected void emitTuple(Message message) {
    byte[] bytes = getBytesFromKafka(message).getBytes();

    if (bytes != null && bytes.length > 0) {
      oport.emit(getBytesFromKafka(message).getBytes());
    }

  }

}
