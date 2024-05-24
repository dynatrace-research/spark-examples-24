package com.dynatrace.spark.basics;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class DatasetExample {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    List<String> items = Arrays.asList("a", "b", "c", "d", "e");
    Dataset<String> dataset = sparkSession.createDataset(items, Encoders.STRING());
    dataset = dataset
        .map((MapFunction<String, String>) String::toUpperCase, Encoders.STRING())
        .filter((FilterFunction<String>) x -> !x.equals("B"));
    dataset.show();
  }

}
