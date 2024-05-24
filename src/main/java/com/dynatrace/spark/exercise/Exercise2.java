package com.dynatrace.spark.exercise;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Exercise2 {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    // TODO
    Dataset<Long> ds = sparkSession.range(100);
  }

}
