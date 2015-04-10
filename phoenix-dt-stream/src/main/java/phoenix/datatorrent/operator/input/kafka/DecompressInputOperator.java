package phoenix.datatorrent.operator.input.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import phoenix.datatorrent.utils.CompressorUtil;

import org.apache.log4j.Logger;

// import pubmatrix.consumer.util.Constants;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;

/**
 * @author Vikas
 */
public class DecompressInputOperator extends BaseOperator {
  private static org.apache.log4j.Logger LOGGER = Logger.getLogger(DecompressInputOperator.class);

  int totalPartitions;
  int partitionId;

  public DecompressInputOperator() {
    // TODO Auto-generated constructor stub
  }

  public DecompressInputOperator(int total, int curr) {
    totalPartitions = total;
    partitionId = curr;
  }


  @Override
  public void setup(OperatorContext context) {}

  public final transient DefaultInputPort<byte[]> in = new DefaultInputPort<byte[]>() {
    @Override
    public void process(byte[] tuple) {
      emitTuple(tuple);
    }
  };

  public transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

  protected void emitTuple(byte[] message) {
    try {

      if (message != null && message.length > 0) {
        byte[] decompressed = CompressorUtil.decompressToString(message);
        if (decompressed != null) {
          System.out.println("Decompressed byte array : "+ decompressed.toString());
          output.emit(decompressed);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Encoding not supported", e);
    }
  }
}
