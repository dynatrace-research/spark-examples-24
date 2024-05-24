package com.dynatrace.spark.wordcount;

import java.io.File;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class DocumentCount {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sparkContext = sparkSession.sparkContext();
    sparkContext.setLogLevel("ERROR");

    File file = new File("./data/loremipsum");
    JavaRDD<String> textFile = sparkContext.textFile(
        file.getPath(), 1).toJavaRDD();
    long count = textFile
        .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .count();
    System.out.println(count);
  }

}
