package phoenix.datatorrent.utils;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.slf4j.LoggerFactory;

/**
 * 
 * @author vikas
 * 
 */
public class CompressorUtil {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CompressorUtil.class);


  public static byte[] compress(byte[] bytesToCompress) throws Exception {
    try {
      Deflater deflater = new Deflater();
      deflater.setInput(bytesToCompress);
      deflater.finish();

      byte[] bytesCompressed = new byte[Short.MAX_VALUE];

      int numberOfBytesAfterCompression = deflater.deflate(bytesCompressed);

      byte[] returnValues = new byte[numberOfBytesAfterCompression];

      System.arraycopy(bytesCompressed, 0, returnValues, 0, numberOfBytesAfterCompression);

      return returnValues;
    } catch (Exception e) {
      LOGGER.error("Compression not supported", e);
      throw new Exception("Compression not supported", e);
    }
  }

  public static byte[] compress(String stringToCompress) throws Exception {
    byte[] returnValues = null;

    try {
      returnValues = compress(stringToCompress.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException uee) {
      LOGGER.error("Encoding not supported ", uee);
      throw new Exception(stringToCompress, uee);
    }

    return returnValues;
  }

  public static byte[] decompress(byte[] bytesToDecompress) throws Exception {
    byte[] returnValues = null;

    Inflater inflater = new Inflater();

    int numberOfBytesToDecompress = bytesToDecompress.length;

    inflater.setInput(bytesToDecompress, 0, numberOfBytesToDecompress);

    int bufferSizeInBytes = numberOfBytesToDecompress;

    List<Byte> bytesDecompressedSoFar = new ArrayList<Byte>();

    try {

      while (!inflater.finished() && !inflater.needsInput()) {
        byte[] bytesDecompressedBuffer = new byte[1024];

        int numberOfBytesDecompressedThisTime = inflater.inflate(bytesDecompressedBuffer);

        for (int b = 0; b < numberOfBytesDecompressedThisTime; b++) {
          bytesDecompressedSoFar.add(bytesDecompressedBuffer[b]);
        }
      }

      returnValues = new byte[bytesDecompressedSoFar.size()];
      for (int b = 0; b < returnValues.length; b++) {
        returnValues[b] = (byte) (bytesDecompressedSoFar.get(b));
      }

    } catch (DataFormatException dfe) {
      LOGGER.error("Data Format Not Recogonized", dfe);
      throw new Exception("Data Format Not Recogonized", dfe);
    }

    inflater.end();

    return returnValues;
  }


  // public static String decompressToString(byte[] bytesToDecompress) throws Exception {
  // byte[] bytesDecompressed = decompress(bytesToDecompress);
  //
  // String returnValue = null;
  //
  // try {
  // returnValue = new String(bytesDecompressed, 0, bytesDecompressed.length, "UTF-8");
  // } catch (UnsupportedEncodingException uee) {
  // LOGGER.error("Encoding not supported", uee);
  // throw new Exception("Encoding not supported", uee);
  // }
  //
  // return returnValue;
  // }


  public static byte[] decompressToString(byte[] bytesToDecompress) throws Exception {
    Inflater inflater = new Inflater();
    inflater.setInput(bytesToDecompress);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream(bytesToDecompress.length);
    try {
      byte[] arr = new byte[18 * 1024];
      while (!inflater.finished() && !inflater.needsInput()) {
        int length = inflater.inflate(arr);
        outStream.write(arr, 0, length);
      }
      outStream.close();
      inflater.end();
    } catch (Exception e) {
      throw e;
    }
    byte[] result = outStream.toByteArray();
   
    return result;
  }
}
