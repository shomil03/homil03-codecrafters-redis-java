import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.File;

public class Main{
  
  static ConcurrentHashMap<String,ValueAndExpiry> map = new ConcurrentHashMap<>();
  static String directoryPath = "";
  static String dbFileName = "";

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

      Parser parser = new Parser(clientSocket.getInputStream());
     
      while (true) {

        String tokens[] = parser.parseNext();

        System.out.println("Received " + Arrays.toString(tokens));

        String response = null;
        
        switch (tokens[0].toLowerCase()) {
          case "echo":
            response = makeBulkString(tokens[1] , false);
            break;
          case "set":
            if(tokens.length > 3)
            map.put(tokens[1] , new ValueAndExpiry(tokens[2], System.currentTimeMillis()+Integer.parseInt(tokens[4])));
            else{
              map.put(tokens[1] , new ValueAndExpiry(tokens[2], Long.MAX_VALUE));
            }
            response = "+OK\r\n";
            break;
          case "get":
            response = handleGet(tokens[1]);
            break;

          case "ping":
            response = makeBulkString("PONG", false);
            break;
          
          case "config":
            response = handleGet(tokens[2]);

          // case "dir":
          //   // path -> filename
          //   directoryPath = tokens[1];
          //   dbFileName = tokens[3];
          //   createFile(directoryPath , dbFileName);
          //   break;

          default:
            break;
        }

        clientSocket.getOutputStream().write(response.getBytes());
        
        
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

  public static void createFile(String path , String fileName) {
    File file = new File(path+fileName);
    try{
      boolean isFileCreated = file.createNewFile();
      if(isFileCreated){
        System.out.println("File created successfully "+ fileName );
      }
      else{
        System.out.println("File creation unsuccessfull");
      }
    }catch(Exception e){
      System.out.println("IOException: "+ e.getMessage());
    }

  }

  public static String makeRESPArray(String a[]) {
    StringBuilder sb = new StringBuilder();
    sb.append("*");
    sb.append(a.length);
    for(String string : a){
      sb.append(makeBulkString(string, false));
    }
    return sb.toString();
  }

  public static String handleGet(String key) {
    if(key.equals("dir")) {
      return makeRESPArray(new String[]{"dir" , directoryPath});
    }
    if(key.equals("dbfilename")) {
      return makeRESPArray(new String[]{key , dbFileName});
    }
    if(!map.containsKey(key)) {
      return makeBulkString("-1" , true);
    }
    
    long currentTime = System.currentTimeMillis();
    long keyExpiryTime = map.get(key).expiry;

    if(currentTime > keyExpiryTime) {
      map.remove(key);
      return makeBulkString("-1" , true);
    }
    return makeBulkString(map.get(key).value , false);
  }
  public static String makeBulkString(String message , boolean nullString){
    StringBuilder sb = new StringBuilder();
    sb.append("$");
    // for(int i = start ; i < message.length ; i++) {
    if(!nullString){
      sb.append(message.length());
      sb.append(addCRLFTreminator());
    }
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
