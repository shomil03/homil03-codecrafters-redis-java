import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class Main extends Thread{
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    Main thread = new Main();
    thread.run();
    //  Uncomment this block to pass the first stage
       
  }

  public void run(){
    ServerSocket serverSocket = null;
       Socket clientSocket = null;
       int port = 6379;
       try {

         serverSocket = new ServerSocket(port);
         // Since the tester restarts your program quite often, setting SO_REUSEADDR
         // ensures that we don't run into 'Address already in use' errors
         serverSocket.setReuseAddress(true);
         // Wait for connection from client.
         clientSocket = serverSocket.accept();

         InputStream input = clientSocket.getInputStream();
         byte[] buffer = new byte[4096];

         while(input.read(buffer) != -1)
         clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
          // if(sc.next().equals("PING")
         
        //  OutputStream output = clientSocket.getOutputStream();
        //  PrintWriter writer = new PrintWriter(output , true);

        //  output.write("+PONG\r\n".getBytes());

        //  writer.println("+PONG\r\n");
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       } finally {
         try {
           if (clientSocket != null) {
             clientSocket.close();
           }
         } catch (IOException e) {
           System.out.println("IOException: " + e.getMessage());
         }
       }
  }
}
