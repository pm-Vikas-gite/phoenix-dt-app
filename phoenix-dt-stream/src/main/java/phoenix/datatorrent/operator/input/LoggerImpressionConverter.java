package phoenix.datatorrent.operator.input;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import phoenix.datatorrent.common.Constants;
import phoenix.datatorrent.model.ImpressionEvent;
import phoenix.datatorrent.utils.Checker;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public class LoggerImpressionConverter extends BaseOperator {
  private static final Logger LOG = LoggerFactory.getLogger(LoggerImpressionConverter.class);


  public final transient DefaultInputPort<byte[]> data = new DefaultInputPort<byte[]>() {
    @Override
    public void process(byte[] tuple) {
      List<GenericRecord> recordList = new ArrayList<GenericRecord>();
      try {
        System.out.println("[LoggerImpressionConverter] : process");
        recordList = getBinaryAvroRecords(tuple);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      emitLine(recordList);
    }
  };


  private List<GenericRecord> getBinaryAvroRecords(byte[] uncompressedData) throws IOException {
    List<GenericRecord> recordList = new ArrayList<GenericRecord>();
  //  Schema schema =
    //    new Schema.Parser().parse(new File("/home/vikas/phoenix/ads-logger-schema.avsc"));
//     Schema schema = new Schema.Parser().parse( new File( "/home/hadoop/phoenix/conf/phoenix-logger-schema.avsc") );
     Schema schema = new Schema.Parser().parse( new File( "/home/hadoop/phoenix/conf/logger.avsc") );

    System.out.println("[LoggerImpressionConverter] : getBinaryAvroRecords");
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
    List<byte[]> binaryAvroRecords = new ArrayList<byte[]>();
    ByteBuffer bb = ByteBuffer.wrap(uncompressedData);
    int capacity = bb.capacity();
    int bbPosition = bb.position();
    System.out.println("capacity : " + capacity + "\t bbPosition : " + bbPosition);
    int position = 0;
    while (position < capacity) {
      byte[] bytes = new byte[4];
      int recordSize = getRecordSize(bb, position, bytes);
      position = position + bytes.length;
      System.out.println("recordSize : " + recordSize);
      if (recordSize != 0) {
        byte[] singleRecord = new byte[recordSize];
        byte[] record = getRecord(bb, position, singleRecord);

        Decoder decoder = DecoderFactory.get().binaryDecoder(record, null);
        GenericRecord result = reader.read(null, decoder);
        recordList.add(result);
        position = position + singleRecord.length;
      }
    }
    return recordList;
  }

  private byte[] getRecord(ByteBuffer bb, int position, byte[] singleRecord) {
    for (int i = position, j = 0; j < singleRecord.length; i++, j++) {
      singleRecord[j] = bb.get(i);
    }
    return singleRecord;
  }

  private int getRecordSize(ByteBuffer bb, int position, byte[] bytes) {
    for (int i = position, j = 0; j < bytes.length; i++, j++) {
      bytes[j] = bb.get(i);
    }
    int recordSize = ByteBuffer.wrap(bytes).getInt();
    return recordSize;
  }

  public final transient DefaultOutputPort<ImpressionEvent> output =
      new DefaultOutputPort<ImpressionEvent>();

  private void emitLine(List<GenericRecord> recordList) {

    try {
      Iterator<GenericRecord> it = recordList.iterator();
      while (it.hasNext()) {
        List<ImpressionEvent> eventList = new ArrayList<ImpressionEvent>();
        
        ImpressionEvent event = null;
        eventList = parseImpression(it.next());
        //event = generateDummyRecords(it.next());
        Iterator<ImpressionEvent> eventItr = eventList.iterator();
        while(eventItr.hasNext()){
          output.emit(eventItr.next());
        }
      }
    } catch (Exception ex) {
      // LOG.error("Failed to parse line {}", line, ex);
      // brokenRecordCount++;
    }
    // emitt only for PMP and RTB,event will be null otherwise
  }

  private ImpressionEvent generateDummyRecords(GenericRecord record) {
    
    ImpressionEvent event = new ImpressionEvent();

    event.setPubId(Checker.check(11111, Integer.class));
    event.setLineItemId(Checker.check(22222, Integer.class));
    event.setCreativeId(Checker.check(33333, Integer.class));
    event.setTargetId(Checker.check(44444, Integer.class));
    event.setAdUnitId(Checker.check(55555, Integer.class));
    event.setTimestamp((Checker.check(System.currentTimeMillis(), Long.class)));
    event.setRecordKey(Constants.IMPRESSIONS);
    
    return event;
  }

  private List<ImpressionEvent> parseImpression(GenericRecord record) {

    GenericRecord impression = (GenericRecord) record.get("impression");
    
    
    @SuppressWarnings("unchecked")
    GenericData.Array<GenericRecord> adRequestArray =
        (Array<GenericRecord>) record.get(Constants.AD_REQUESTS);
    List<ImpressionEvent> eventList = new ArrayList<ImpressionEvent>();
    
    
    
    if (adRequestArray != null && !adRequestArray.isEmpty()) {
      for (int i = 0; i < adRequestArray.size(); i++) {
        ImpressionEvent event = new ImpressionEvent();
        
        if (adRequestArray.get(i).get(Constants.LINE_ITEM_ID) != null) {
          event.setLineItemId(Checker.check(adRequestArray.get(i).get(Constants.LINE_ITEM_ID), Integer.class));
        } else {
          event.setLineItemId(Checker.check(0, Integer.class));
        }
        
        if (adRequestArray.get(i).get(Constants.CREATIVE_ID) != null) {
          event.setCreativeId(Checker.check(adRequestArray.get(i).get(Constants.CREATIVE_ID), Integer.class));
        } else {
          event.setCreativeId(Checker.check(0, Integer.class));
        }
        
        if (adRequestArray.get(i).get(Constants.TARGET_ID) != null) {
          event.setTargetId(Checker.check(adRequestArray.get(i).get(Constants.TARGET_ID), Integer.class));
        } else {
          event.setTargetId(Checker.check(0, Integer.class));
        }
        
        if (adRequestArray.get(i).get(Constants.AD_UNIT_ID) != null) {
          event.setAdUnitId(Checker.check(adRequestArray.get(i).get(Constants.AD_UNIT_ID), Integer.class));
        } else {
          event.setAdUnitId(Checker.check(0, Integer.class));
        }
        
        if (impression.get(Constants.TIMESTAMP) != null) {
          event.setTimestamp((Checker.check(impression.get(Constants.TIMESTAMP), Long.class)) * 1000);
        } 
        
        LOG.info("TIMESTAMP : ",impression.get(Constants.TIMESTAMP).toString());
        
        event.setRecordKey(Constants.IMPRESSIONS);
        eventList.add(event);
      }
    }
    return eventList;
  }

  @Override
  public void endWindow() {
    super.endWindow();
  }
}
