package com.dynatrace.spark.exercise;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Exercise3 {

  public static void main(String[] args) {
    SparkConf conf =
        new SparkConf().setAppName("MyApp").setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    // TODO

    long numPartitions = 0;
    System.out.println("number of partitions: " + numPartitions);

    long numRows = 0;
    System.out.println("number of rows in partition 0: " + numRows);

    long numRows10 = 0;
    System.out.println("number of rows in partition 0 (with 10 partitions): " + numRows10);
  }

}
