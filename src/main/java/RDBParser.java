import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RDBParser {
    // public static List<String> readRDBFile( String file) {
        static HashMap<String , String[]> map = new HashMap<>();
        public static String[] readRDBFile( String file) {
        List<String> keys = new ArrayList<>();
        String key ="";
        String value = "";
        try{
            InputStream fis = new FileInputStream(new File(file));
              byte[] redis = new byte[5];
              byte[] version = new byte[4];
              fis.read(redis);
              fis.read(version);
              System.out.println("Magic String = " +
                                 new String(redis, StandardCharsets.UTF_8));
              System.out.println("Version = " +
                                 new String(version, StandardCharsets.UTF_8));
              int b;
            header:
              while ((b = fis.read()) != -1) {
                switch (b) {
                case 0xFF:
                  System.out.println("EOF");
                  break;
                case 0xFE:
                  System.out.println("SELECTDB");
                  break;
                case 0xFD:
                  System.out.println("EXPIRETIME");
                  break;
                case 0xFC:
                  System.out.println("EXPIRETIMEMS");
                  break;
                case 0xFB:
                  System.out.println("RESIZEDB");
                  b = fis.read();
                  fis.readNBytes(lengthEncoding(fis, b));
                  fis.readNBytes(lengthEncoding(fis, b));
                  break header;
                case 0xFA:
                  System.out.println("AUX");
                  break;
                }
              }
              System.out.println("header done");
              // now key value pairs
              while ((b = fis.read()) != -1 && ( b != 255)) { // value type
                System.out.println("value-type = " + b);
                b = fis.read();
                int valueType = b;
                System.out.println("value-type = " + b);
                //                                b = fis.read();
                //                                System.out.println("value-type
                //                                = " + b);
                System.out.println(" b = " + Integer.toBinaryString(b));

                System.out.println("reading keys");
                int strLength = lengthEncoding(fis, b);
                b = fis.read();
                System.out.println("strLength == " + strLength);
                if (strLength == 0) {
                  strLength = b;
                }
                System.out.println("strLength == " + strLength);
                byte[] bytes = fis.readNBytes(strLength);
                key = new String(bytes);
                keys.add(key);

                // decoding values
                // switch  case to decode different type of data
                switch(valueType) {
                  // when data is list type
                  case 9:
                    List<String> list = new ArrayList<>();
                    int listLength = lengthEncoding(fis, b);
                    System.out.println("List length: "+ listLength );

                    for(int i = 0 ; i < listLength ; i++) {
                      int elementLength = lengthEncoding(fis, fis.read());
                      byte[] elementByte = fis.readNBytes(elementLength);
                      String element = new String(elementByte , StandardCharsets.UTF_8);
                      list.add(element);
                    }
                    map.put(key ,list.toArray(new String[0]));
                    break;

                  case 5:
                  case 10:
                    System.out.println("Encoding zipList"); 
                    int ziplistLength = lengthEncoding(fis, fis.read());
                    byte[] ziplistBytes = fis.readNBytes(ziplistLength);
                    int index = 0;
                    
                    int totalLength = decodeLength(ziplistBytes, index);
                    index += lengthBytesUsed(totalLength);

                    int numberOfEntries = decodeLength(ziplistBytes, index);
                    index += lengthBytesUsed(numberOfEntries);

                    List<String> listValues = new ArrayList<>();
                    for(int i = 0 ; i < numberOfEntries ; i++) {

                      int elementLength = decodeLength(ziplistBytes, index);
                      index += lengthBytesUsed(elementLength);

                      String element = new String(ziplistBytes , index , elementLength , StandardCharsets.UTF_8);
                      index += elementLength;

                      listValues.add(element);
                      System.out.println("ZipList Entry: " + element);

                    }
                    map.put(key, listValues.toArray(new String[0]));
                    System.out.println("Decoded ziplist for key " + key + ": " + listValues);
                    break;
                    

                  case 6: // Hash encoded as Ziplist
                    System.out.println("Value type: Hash (Ziplist)");
                    ziplistLength = lengthEncoding(fis, fis.read());
                    ziplistBytes = fis.readNBytes(ziplistLength);

                    index = 0;
                    Map<String, String> hash = new HashMap<>();
                    while (index < ziplistBytes.length) {
                        int fieldLength = decodeLength(ziplistBytes, index);
                        index += lengthBytesUsed(fieldLength);

                        String field = new String(ziplistBytes, index, fieldLength, StandardCharsets.UTF_8);
                        index += fieldLength;

                        int valueLength = decodeLength(ziplistBytes, index);
                        index += lengthBytesUsed(valueLength);

                        value = new String(ziplistBytes, index, valueLength, StandardCharsets.UTF_8);
                        index += valueLength;

                        hash.put(field, value);
                        System.out.println("Field: " + field + ", Value: " + value);
                    }
                    map.put(key, new String[]{hash.toString()});
                    break;
                  
                  default:
                    int valueLength = lengthEncoding(fis, b);
                    b = fis.read();
                    if(valueLength == 0) {
                      valueLength = b;
                    }
                    bytes = fis.readNBytes(valueLength);
                    value = new String(bytes);
                    System.out.println("Value for string : " + value +" key : "+ key);
                    map.put(key, new String[]{new String(value)});
                    break;


                }
                
                
                
                
                
                
                System.out.println("Map inside fileReader: "+ map);
                // break;

              }
        }catch(Exception e) {
            System.out.println("Error in reading file content: " + e.getMessage());
        }
        System.out.println("keys: " + keys);
        return keys.toArray(new String[keys.size()]);
            // out.printf("*1\r\n$%d\r\n%s\r\n", key.length(), key);
            // out.flush();
    }

    private static String readHeader(FileInputStream inputStream, byte[] buffer) throws IOException {
        byte[] header = new byte[9];
        int bytesRead = inputStream.read(header);
        if (bytesRead != 9) {
            throw new IOException("Invalid RDB file header");
        }
        return new String(header);
    }
    private static int lengthEncoding(InputStream is, int b) throws IOException {
    int length = 100;
    int first2bits = b & 11000000;
    if (first2bits == 0) {
      System.out.println("00");
      length = 0;
    } else if (first2bits == 128) {
      System.out.println("01");
      length = 2;
    } else if (first2bits == 256) {
      System.out.println("10");
      ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
      buffer.put(is.readNBytes(4));
      buffer.rewind();
      length = 1 + buffer.getInt();
    } else if (first2bits == 256 + 128) {
      System.out.println("11");
      length = 1; // special format
    }
    return length;
  }
  private static int decodeLength(byte[] ziplist, int index) {
      int length = ziplist[index] & 0xFF; // Single-byte length
      if ((length & 0xC0) == 0xC0) { // Multi-byte length
          length = ((ziplist[index] & 0x3F) << 8) | (ziplist[index + 1] & 0xFF);
      }
      return length;
  }
  private static int lengthBytesUsed(int length) {
    return (length & 0xC0) == 0xC0 ? 2 : 1;
  }

}