package com.dynatrace.spark.basics;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlExample {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    List<String> items = Arrays.asList("a", "b", "c", "d", "e");
    Dataset<String> dataset = sparkSession.createDataset(items, Encoders.STRING());
    dataset.createOrReplaceTempView("data");
    Dataset<Row> result = sparkSession.sql(
        "SELECT UPPER(value) as value FROM data WHERE UPPER(value) != \"B\"");
    result.show();
  }

}
