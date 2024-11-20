import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Scanner;

public class Main{
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
          // if(sc.next().equals("PING")
         
        //  OutputStream output = clientSocket.getOutputStream();
        //  PrintWriter writer = new PrintWriter(output , true);

        //  output.write("+PONG\r\n".getBytes());

        //  writer.println("+PONG\r\n");
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
        System.out.println(Arrays.toString(messgeString));
        // messgeString.toLowerCase();
        // if(messgeString.equals("echo")) {
          // String RESPbulkString = makeRESPbuldString()
        // }
        clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
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
}
