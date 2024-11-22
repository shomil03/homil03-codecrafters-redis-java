import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RDBParser {
    public static List<String> readRDBFile( String file) {
        List<String> keys = new ArrayList<>();
        try{
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String header = readHeader(reader);
            System.out.println("Header: "+ header);
            while(true) {

                int type = reader.read();

                if(type == -1) break;

                if(type == 0xFF) {
                    break;
                }


                if(type == 0x00) {
                    String key = readLengthEncodedString(reader);
                    String value = readLengthEncodedString(reader);
                    keys.add(value);
                    System.out.println("key in file : "+ key +"\n" + "value in file: "+ value);
                }
                
            }
            return keys;

        }catch(Exception e) {
            System.out.println("Error reading file content: " + e.getMessage());
        }

        return keys;
    }

    public static String readHeader(BufferedReader reader) throws IOException {
        char[] header = new char[9];

        return new String(header);
    }

    public static String readLengthEncodedString(BufferedReader reader) throws IOException {

        int length = readLength(reader);
        char data[] = new char[length];
        int read = reader.read(data);
        return new String(data);
    }


    public static int readLength(BufferedReader reader) throws IOException {
        int firstByte = reader.read();

        if((firstByte & 0xC0) == 0x00) {
            return firstByte & 0x3F;
        }
        else if((firstByte & 0xC0) == 0x40) {
            int secondByte = reader.read();
            return ((firstByte & 0x3F) << 8) | secondByte;
        }
        else if(firstByte == 0x80) {
            throw new IOException("Encoded lengths are not supported in this parser.");
        }
        else{
            throw new IOException("Invalid length encoding.");
        }
    }
}
