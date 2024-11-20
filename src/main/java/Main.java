import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Scanner;
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
      InputStream input = clientSocket.getInputStream();
      byte[] buffer = new byte[4096];
      int byteRead;
      while((byteRead = input.read(buffer)) != -1){
        String messgeString[] = new String(buffer , 0 , byteRead , "UTF-8").trim().split("[^a-zA-Z]+");

        String command = parseCommand(messgeString).toLowerCase();
        switch (command) {
          case "echo":
            clientSocket.getOutputStream().write(makeBulkString(messgeString,2).getBytes());
            return;
          case "set":
            map.put(messgeString[2] , messgeString[3]);
            clientSocket.getOutputStream().write("+OK\r\n".getBytes());
            return;
          case "get":
            if(map.containsKey(messgeString[2])) {
              clientSocket.getOutputStream().write(makeBulkString(messgeString,3).getBytes());
              return;
            }
            clientSocket.getOutputStream().write("+OK\r\n".getBytes());
        
          default:
            clientSocket.getOutputStream().write("$-1\r\n".getBytes());
            break;
        }
        System.out.println(Arrays.toString(messgeString));
        
        
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
  public static String makeBulkString(String message[],int start){
    StringBuilder sb = new StringBuilder();
    sb.append("$");
    boolean flag = false;
    for(int i = start ; i < message.length ; i++) {
      sb.append(message[i].length());
      sb.append(addCRLFTreminator());
      sb.append(message[i]);
      sb.append(addCRLFTreminator());
    }
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
