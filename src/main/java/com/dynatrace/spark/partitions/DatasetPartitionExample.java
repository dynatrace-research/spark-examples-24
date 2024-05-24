package com.dynatrace.spark.partitions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.spark_partition_id;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class DatasetPartitionExample {

  public static void main(String[] args) {
    SparkConf conf =
        new SparkConf().setAppName("MyApp").setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    Dataset<Long> numbers = sparkSession.range(0, 100);
    Dataset<Long> dataset = numbers.filter(col("id").mod(2).equalTo(0L));
    System.out.println("number of items: " + dataset.count());

    // count the RDD partitions
    System.out.println(dataset.rdd().getNumPartitions());

    // count the Spark partition ids
    System.out.println(dataset.select(spark_partition_id()).as("id").distinct().count());
  }

}



