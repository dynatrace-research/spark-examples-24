package com.dynatrace.spark.streaming.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class LoremIpsumInput {

  public static void main(String[] args) {
    // test file
    File input = new File("./data/loremipsum");

    try (ServerSocket serverSocket = new ServerSocket(1234);
         BufferedReader reader = new BufferedReader(new FileReader(input))) {

      System.out.println("Waiting for connection");
      Socket socket = serverSocket.accept();
      System.out.println("Connection accepted.");
      OutputStream outputStream = socket.getOutputStream();

      String line;
      while ((line = reader.readLine()) != null) {
        outputStream.write((line + "\n").getBytes());
        System.out.println(line);
        Thread.sleep(5_000);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }

}
