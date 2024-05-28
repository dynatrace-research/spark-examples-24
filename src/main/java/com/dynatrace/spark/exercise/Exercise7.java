package com.dynatrace.spark.exercise;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class Exercise7 {

  private static final String VALUE = "value";
  private static final String COUNT = "count";
  private static final String TOTAL_COUNT = "totalCount";
  private static final String PART_SEEN = "partSeen";

  public static void main(String[] args) throws TimeoutException, StreamingQueryException {

    // INITIALIZATION

    SparkConf conf = new SparkConf()
        .setAppName("WordCount")
        .setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");


    // DATASET FROM FILE

    Dataset<Row> completeDataset = sparkSession.read().textFile("./data/loremipsum")
        .flatMap((FlatMapFunction<String, String>) s ->
            Arrays.stream(s.split("[^a-zA-Z\\d]")).iterator(), Encoders.STRING())
        .filter(col(VALUE).notEqual(""))
        .map((MapFunction<String, String>) (String::toLowerCase), Encoders.STRING())
        .groupBy(VALUE)
        .count()
        .withColumnRenamed(COUNT, TOTAL_COUNT);
    completeDataset.cache();


    // DATASET FROM STREAM

    Dataset<Row> streamInput = sparkSession
        .readStream()
        .format("socket")
        .option("host", "localhost")
        .option("port", "1234")
        .load();
    Dataset<Row> words = streamInput.as(Encoders.STRING())
        .flatMap((FlatMapFunction<String, String>) s ->
            Arrays.asList(s.split("[^a-zA-Z\\d]")).iterator(), Encoders.STRING())
        .filter(col(VALUE).notEqual(""))
        .map((MapFunction<String, String>) (String::toLowerCase), Encoders.STRING())
        .groupBy(VALUE)
        .count();


    // JOIN

    Dataset<Row> counts = words
        .join(completeDataset, VALUE)
        .withColumn(PART_SEEN, round(col(COUNT).multiply(100).divide(col(TOTAL_COUNT)), 1))
        .sort(col(PART_SEEN).desc(), col(COUNT).desc())
        .withColumn(PART_SEEN, concat(col(PART_SEEN), lit("%")));
    StreamingQuery streamingQuery = counts.writeStream()
        .outputMode("complete")
        .format("console")
        .start();
    streamingQuery.awaitTermination();
  }

}
