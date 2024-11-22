import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class RDBParser {
    public static List<String> readRDBFile( String file) {
        List<String> keys = new ArrayList<>();
        try{




            
            FileInputStream inputStream = new FileInputStream(file);
            byte[] buffer = new byte[8192]; // Buffer size (8 KB)
            int bytesRead;
            int bufferIndex = 0;
            int bufferEnd = 0;

            // Read the header (first 9 bytes)
            String header = readHeader(inputStream, buffer);
            System.out.println("Header: " + header);

            while (true) {
                // Ensure buffer has data to read
                if (bufferIndex >= bufferEnd) {
                    bytesRead = inputStream.read(buffer);
                    if (bytesRead == -1) {
                        break; // End of file
                    }
                    bufferIndex = 0;
                    bufferEnd = bytesRead;
                }

                // Read the type byte
                int type = buffer[bufferIndex++] & 0xFF;

                if (type == 0xFF) {
                    System.out.println("Break type 0xFF");
                    break; // End of RDB file
                }

                if (type == 0x00) {
                    String key = readLengthEncodedString(buffer, inputStream, bufferIndex, bufferEnd);
                    String value = readLengthEncodedString(buffer, inputStream, bufferIndex, bufferEnd);
                    keys.add(value);
                    System.out.println("Key in file: " + key + "\nValue in file: " + value);
                }
            }
        } catch (Exception e) {
            System.out.println("Error reading file content: " + e.getMessage());
        }
        return keys;
    }

    private static String readHeader(FileInputStream inputStream, byte[] buffer) throws IOException {
        byte[] header = new byte[9];
        int bytesRead = inputStream.read(header);
        if (bytesRead != 9) {
            throw new IOException("Invalid RDB file header");
        }
        return new String(header);
    }

    private static String readLengthEncodedString(byte[] buffer, FileInputStream inputStream, int bufferIndex, int bufferEnd) throws IOException {
        int length = readLength(buffer, inputStream, bufferIndex, bufferEnd);
        byte[] data = new byte[length];
        int bytesRead = inputStream.read(data);
        if (bytesRead != length) {
            throw new IOException("Error reading length-encoded string");
        }
        return new String(data);
    }

    private static int readLength(byte[] buffer, FileInputStream inputStream, int bufferIndex, int bufferEnd) throws IOException {
        int firstByte;
        if (bufferIndex >= bufferEnd) {
            firstByte = inputStream.read();
            if (firstByte == -1) {
                throw new IOException("Unexpected end of file");
            }
        } else {
            firstByte = buffer[bufferIndex++] & 0xFF;
        }

        if ((firstByte & 0xC0) == 0x00) {
            return firstByte & 0x3F;
        } else if ((firstByte & 0xC0) == 0x40) {
            int secondByte = inputStream.read();
            if (secondByte == -1) {
                throw new IOException("Unexpected end of file");
            }
            return ((firstByte & 0x3F) << 8) | secondByte;
        } else if (firstByte == 0x80) {
            throw new IOException("Encoded lengths are not supported in this parser.");
        } else {
            throw new IOException("Invalid length encoding.");
        }
    }
}