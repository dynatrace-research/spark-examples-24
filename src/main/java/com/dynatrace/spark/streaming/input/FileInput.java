package com.dynatrace.spark.streaming.input;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;

public class FileInput {

  public static void main(String[] args) {

    try (ServerSocket serverSocket = new ServerSocket(1234)) {

      System.out.println("Waiting for connection");
      Socket socket = serverSocket.accept();
      System.out.println("Connection accepted.");
      OutputStream outputStream = socket.getOutputStream();

      // test files
      File baseDir = new File("./data/testExample");

      // send the content of one file at a time
      for (File f : Objects.requireNonNull(baseDir.listFiles())) {
        List<String> lines = Files.readAllLines(f.toPath());
        String fileContent = lines.stream().reduce((a, b) -> a + " " + b).orElse("");
        outputStream.write((fileContent + "\n").getBytes());

        System.out.println(fileContent);

        Thread.sleep(10_000);
      }

    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }

}
