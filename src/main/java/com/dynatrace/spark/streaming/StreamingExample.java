package com.dynatrace.spark.streaming;

import java.util.concurrent.TimeoutException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamingExample {

  public static void main(String[] args) throws TimeoutException, StreamingQueryException {
    SparkConf conf = new SparkConf()
        .setAppName("WordCount")
        .setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    // open the input stream
    Dataset<Row> input = sparkSession
        .readStream()
        .format("socket")
        .option("host", "localhost")
        .option("port", "1234")
        .load();

    // process the Dataset
    Dataset<String> words = input.as(Encoders.STRING())
        .map((MapFunction<String, String>) (String::toUpperCase), Encoders.STRING());

    // output result
    StreamingQuery streamingQuery = words.writeStream()
        .outputMode("complete")
        .format("console")
        .start();

    streamingQuery.awaitTermination();
  }

}
