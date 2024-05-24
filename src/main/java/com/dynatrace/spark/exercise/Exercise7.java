package com.dynatrace.spark.exercise;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Exercise7 {

  public static void main(String[] args) {

    // INITIALIZATION

    SparkConf conf = new SparkConf()
        .setAppName("WordCount")
        .setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    // TODO
    // DATASET FROM FILE

    // TODO
    // DATASET FROM STREAM

    // TODO
    // JOIN
  }

}
