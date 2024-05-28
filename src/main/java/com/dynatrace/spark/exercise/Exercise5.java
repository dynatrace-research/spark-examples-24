package com.dynatrace.spark.exercise;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

public class Exercise5 {

  private static final String VALUE = "value";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("WordCount")
        .setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    Dataset<String> combined = sparkSession.emptyDataset(Encoders.STRING());

    File dir = new File("./data/testExample");
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }

    for (File file : files) {
      Dataset<String> d = sparkSession.read().textFile(file.getAbsolutePath())
          .flatMap((FlatMapFunction<String, String>) s ->
              Arrays.stream(s.toLowerCase().split("[^a-zA-Z\\d]")).iterator(), Encoders.STRING());
      combined = combined.union(d);
    }

    Dataset<Row> result = combined
        .filter(col(VALUE).notEqual(""))
        .groupBy(VALUE)
        .count()
        .sort(col("count").desc());
    result.show();
  }

}
