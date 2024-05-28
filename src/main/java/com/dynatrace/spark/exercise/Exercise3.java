package com.dynatrace.spark.exercise;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.spark_partition_id;

public class Exercise3 {

  private static final String PARTITION_ID = "partitionId";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    Dataset<Row> numbers = sparkSession.range(0, 1000).withColumn(PARTITION_ID, spark_partition_id());

    long numPartitions = numbers.select(col(PARTITION_ID)).distinct().count();
    System.out.println("number of partitions: " + numPartitions);

    long numRows = numbers.filter(col(PARTITION_ID).equalTo(0L)).count();
    System.out.println("number of rows in partition 0: " + numRows);

    long numRows10 = numbers.repartition(10).filter(spark_partition_id().equalTo(0)).count();
    System.out.println("number of rows in partition 0 (with 10 partitions): " + numRows10);
  }

}
