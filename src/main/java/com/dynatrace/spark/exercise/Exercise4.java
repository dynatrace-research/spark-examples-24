package com.dynatrace.spark.exercise;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

public class Exercise4 {

  private static final String VALUE = "value";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("WordCount")
        .setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    Dataset<Row> dataset = sparkSession.read().textFile("./data/loremipsum")
        .flatMap((FlatMapFunction<String, String>) s ->
            Arrays.stream(s.split("[^a-zA-Z\\d]")).iterator(), Encoders.STRING())
        .filter(col(VALUE).notEqual(""))
        .map((MapFunction<String, String>) (String::toLowerCase), Encoders.STRING())
        .groupBy(VALUE)
        .count()
        .sort(col("count").desc());
    dataset.show(15);
  }

}
