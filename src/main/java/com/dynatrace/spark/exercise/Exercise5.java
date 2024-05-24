package com.dynatrace.spark.exercise;

import java.io.File;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Exercise5 {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("WordCount")
        .setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    // TODO
    File dir = new File("./data/testExample");
    File[] files = dir.listFiles();
  }

}
