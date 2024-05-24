package com.dynatrace.spark.operations;

import static org.apache.spark.sql.functions.sum;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Functions {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("WordCount")
        .setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    Dataset<Long> data = sparkSession.range(100);

    Long x = data.reduce((ReduceFunction<Long>) Long::sum);
    System.out.println("sum: " + x);

    data.select(sum(data.col("id")).as("sum")).show();
  }

}
