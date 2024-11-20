import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
public class Main{
  static HashMap<String,String> map = new HashMap<>();
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

       ServerSocket serverSocket = null;
      //  Socket clientSocket = null;
       int port = 6379;
       try {

         serverSocket = new ServerSocket(port);
         // Since the tester restarts your program quite often, setting SO_REUSEADDR
         // ensures that we don't run into 'Address already in use' errors
         serverSocket.setReuseAddress(true);
         // Wait for connection from client.

         while(true) {
          final Socket clientSocket = serverSocket.accept();
          new Thread(() -> handleClient(clientSocket)).start();
         }
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
      //  } finally {
      //    try {
      //      if (clientSocket != null) {
      //        clientSocket.close();
      //      }
      //    } catch (IOException e) {
      //      System.out.println("IOException: " + e.getMessage());
      //    }
       }
       
  }
  static void handleClient(Socket clientSocket) {
    try{
      BufferedReader input = new BufferedReader(
          new InputStreamReader(clientSocket.getInputStream()));

      String line = null;
      while((line = input.readLine())  != null) {
      line.toLowerCase();
      System.out.println(line);
      // byte[] buffer = new byte[4096];
      // int byteRead;
      // while((byteRead = input.read(buffer)) != -1){
        // String messgeString[] = new String(buffer , 0 , byteRead , "UTF-8").trim().split("[^a-zA-Z]+");
        // System.out.println(Arrays.toString(messgeString));
        // String command = parseCommand(messgeString).toLowerCase();
        // System.out.println("Command = " + command);
        switch (line) {
          case "echo":
            String message = input.readLine();
            clientSocket.getOutputStream().write(makeBulkString(message).getBytes());
            return;
          case "set":
            String key = input.readLine();
            String value = input.readLine();
            map.put(key , value);
            clientSocket.getOutputStream().write("+OK\r\n".getBytes());
            return;
          case "get":
            key = input.readLine();
            System.out.println("In get");
            if(map.containsKey(key)) {
              System.out.println("fetching key for "+ key + " " + map.get(key));
              clientSocket.getOutputStream().write(makeBulkString(map.get(key)).getBytes());
              return;
            }
            clientSocket.getOutputStream().write("$-1\r\n".getBytes());
            break;

          default:
            clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
            break;
        }
        // System.out.println(Arrays.toString(messgeString));÷
        
        
      }
    }catch(Exception e){
      System.out.println("Client handler exception: " + e.getMessage());
    }
    finally{
      try {
        clientSocket.close();
        System.out.println("Client disconnected");
    } catch (IOException e) {
        System.out.println("Error closing client socket: " + e.getMessage());
    }
    }
  }
  public static String makeBulkString(String message){
    StringBuilder sb = new StringBuilder();
    sb.append("$");
    // for(int i = start ; i < message.length ; i++) {
    sb.append(message.length());
    sb.append(addCRLFTreminator());
    sb.append(message);
    sb.append(addCRLFTreminator());
    // }
    return sb.toString();
  }
  public static String addCRLFTreminator(){
    return "\r\n";
  }
  public static String parseCommand(String[] message) {
    for(String s : message) {
      if(s.length() > 0) {
        return s;
      }
    }
    return "";
  }
}
