import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.io.File;
import java.io.FileReader;

public class Main{
  static List<OutputStream> replicaOutputStreams = new ArrayList<>();
  static ConcurrentHashMap<String,ValueAndExpiry> map = new ConcurrentHashMap<>();
  static String directoryPath = null;
  static String dbFileName = null;
  static String role = "master";
  static int masterPort = 6379;
  static int slavePort = -1;
  static String master_replicationID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  static String master_replicationOffset = "0";
  static String masterIP = "";
  static String hostName = "";
  static String slaveName = "localhost";
  static int port = 6379;
  // static Socket slaveSocket;
  static OutputStream slaveOutput;
  static Queue<String[]> queue = new LinkedList<>();
  
    public static void main(String[] args){
      // You can use print statements as follows for debugging, they'll be visible when running tests.
      System.out.println("Logs from your program will appear here!");
      
      
      for(int i = 0 ; i < args.length ; i++){
  
        
        if(args[i].equals("--replicaof")) {
          role = "slave";
          i++;
         
          masterPort = Integer.parseInt(args[i].split(" ")[1]);
          hostName = args[i].split(" ")[0];
           
  
          System.out.println("Master Port: "+ masterPort);
          System.out.println("Hostname: "+ hostName);
  
  
          try{
            Socket slaveSocket = new Socket(hostName , masterPort);
            slaveOutput = slaveSocket.getOutputStream();
            String pingMaster = "*1\r\n$4\r\nPING\r\n";
            slaveSocket.getOutputStream().write(pingMaster.getBytes());
            slaveSocket.getInputStream().read();
            slaveSocket.getOutputStream().flush();
  
            String sendRRPLCONF = makeRESPArray(new String[]{"REPLCONF" , "listening-port", ""+port});
            slaveSocket.getOutputStream().write(sendRRPLCONF.getBytes());
            slaveSocket.getInputStream().read();
            slaveSocket.getOutputStream().flush();
  
            String sendCAPA = makeRESPArray(new String[]{"REPLCONF" ,"capa" , "psync2" });
            slaveSocket.getOutputStream().write(sendCAPA.getBytes());
            slaveSocket.getInputStream().read();
            slaveSocket.getOutputStream().flush();
  
            String sendPSYNC = makeRESPArray(new String[]{"PSYNC" ,"?", "-1"});
            slaveSocket.getOutputStream().write(sendPSYNC.getBytes());
            // slaveSocket.getInputStream().read();
            // slaveSocket.getOutputStream().flush();
  
          }catch(Exception e) {
            System.out.println("error in connection to master port: "+ e.getMessage());
          }
  
        }
        if(args[i].equals("--port")) {
          port = Integer.parseInt(args[i+1]);
        }
        if(args[i].equals("--dir")){
          directoryPath = args[i+1];
        }
        if(args[i].equals("--dbfilename")){
          dbFileName = args[i+1];
        }
      }
         ServerSocket serverSocket = null;
         
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

    final static class RespondTask{
      final OutputStream out;
      public RespondTask(OutputStream out) {
        this.out = out;
        Main.replicaOutputStreams.add(out);
      }
    }
    static void handleClient(Socket clientSocket) {
      BlockingQueue<String[]> blockingQueue = new LinkedBlockingDeque<>();
      
      try{
        Parser parser = new Parser(clientSocket.getInputStream());
        while (true) {
  
          String tokens[] = parser.parseNext();
  
          System.out.println("Received " + Arrays.toString(tokens));
          System.out.println(directoryPath +" "+ dbFileName);
          String response = "";
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

              System.out.println("Replica outputstream size :" + Main.replicaOutputStreams.size());
              for(int i= 0 ; i < Main.replicaOutputStreams.size() ; i++){
                Main.replicaOutputStreams.get(i).write(makeRESPArray(tokens).getBytes());
              }
  
              break;
            case "get":
              response = handleGet(tokens[1]);
              break;
  
            case "ping":
              response = makeBulkString("PONG", false);
              break;
            
            case "config":
              response = handleGet(tokens[2]);
              break;
            
            case "keys":
              RDBParser filParser = new RDBParser();
              String keys[] = filParser.readRDBFile(directoryPath +"/" + dbFileName);
              response = makeRESPArray(keys);
              break;
  
            case "info":
                response = makeBulkString("role:"+role+"master_replid:"+master_replicationID+"master_repl_offset:"+master_replicationOffset, false);
               break;
  
            case "replconf":
                response = "+OK\r\n";
                break;  
  
            case "psync":
                  
                  // try{
                    byte[] bytes = HexFormat.of().parseHex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
                    
                    response = makeBulkString("+FULLRESYNC "+master_replicationID+" "+master_replicationOffset+"\r\n", false);
                    clientSocket.getOutputStream().write(response.getBytes());
                    response = "$"+bytes.length+"\r\n";
                    clientSocket.getOutputStream().write(response.getBytes());
                    clientSocket.getOutputStream().write(bytes);
                    response = null;

                    // try{
                    //   System.out.println("Started Blocking queue");
                    //   while(true){
                    //     String[] element = blockingQueue.take();
                    //     System.out.println("Element retrieved from queue: "+ element.toString());
                    //     clientSocket.getOutputStream().write(makeRESPArray(element).getBytes());
                    //   }
                    // }catch(Exception e){
                    //   System.out.println("Error in blocking queue: "+ e.getMessage());
                    // }

                    // RespondTask task = new RespondTask(clientSocket.getOutputStream());
                    Main.replicaOutputStreams.add(clientSocket.getOutputStream());

                
                  break;
  
            default:
              break;
          }
          
  
          if(response != null)
          clientSocket.getOutputStream().write(response.getBytes());
          
          
        }
      }catch(Exception e){
        System.out.println("Client handler exception: " + e.getMessage());
      }
      finally{
        
    //   try {
    //     // System.out.println("Sending slave from queue");
    //     // while(!queue.isEmpty()) {
    //       // System.out.println("-> "+ Arrays.toString(queue.peek()));
    //       // clientSocket.getOutputStream().write(makeRESPArray(queue.remove()).getBytes());
    //     // queue.remove();
    //     // }
    //     // clientSocket.close();
    //     System.out.println("Client disconnected");
    // } catch (IOException e) {
    //     System.out.println("Error closing client socket: " + e.getMessage());
    // }
    }
  }

 

  public static void readFile() {
    try{
      BufferedReader reader = new BufferedReader(new FileReader(directoryPath+"/"+dbFileName));

      String line = "";
      while((line = reader.readLine()) != null) {
        System.out.println("File content: " + Arrays.toString(line.getBytes(StandardCharsets.ISO_8859_1)));
      }
    }catch(Exception e){
      System.out.println("Error in reading file content");
      System.out.println("IOException: " + e.getMessage());
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
    sb.append(addCRLFTreminator());
    for(String string : a){
      // if(string.equals("")){
      //   sb.append(makeBulkString("-1", true));
      //   continue;
      // }
      sb.append(makeBulkString(string, false));
    }
    return sb.toString();
  }

  public static String handleGet(String key) {
    if(key.equals("dir")) {
      if(directoryPath == null){
        return makeRESPArray(new String[]{});
        // return makeBulkString("-1", true);
      }
      return makeRESPArray(new String[]{"dir" , directoryPath});
    }
    if(key.equals("dbfilename")) {
      if(dbFileName == null){
        return  makeRESPArray(new String[]{});
        // return makeBulkString("-1", true);
      }

      return makeRESPArray(new String[]{key , dbFileName});
    }
    if(!map.containsKey(key)) {
      // check key in file
      RDBParser fileReader = new RDBParser();
      fileReader.readRDBFile( directoryPath + "/"+ dbFileName);
      // System.out.println("Map in fileReader: "+ fileReader.map);
      if(fileReader.map.containsKey(key)){
        System.out.println("Key found in file: ");
          return handleKeyWithExpiry(key, fileReader.map.get(key).expiry , fileReader.map.get(key).value);
      }

      return makeBulkString("-1" , true);
    }
    
    // handle key-value with expiry

    return handleKeyWithExpiry(key, map.get(key).expiry , map.get(key).value);
    
  }

  public static String handleKeyWithExpiry(String key, long keyExpiryTime , String value) {
    System.out.println("key expiry Time: " + keyExpiryTime);

    long currentTime = System.currentTimeMillis();
    if(currentTime > keyExpiryTime) {
      map.remove(key);
      return makeBulkString("-1", true);
    }
    return makeBulkString(value, false);
  }

  public static String makeBulkString(String message , boolean nullString){
    StringBuilder sb = new StringBuilder();
    sb.append("$");
    if(message == null) {
      sb.append("0");
      sb.append(addCRLFTreminator());
      sb.append(addCRLFTreminator());
      return sb.toString();
    }
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
