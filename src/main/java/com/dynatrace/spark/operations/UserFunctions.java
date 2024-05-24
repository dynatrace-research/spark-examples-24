package com.dynatrace.spark.operations;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

public class UserFunctions {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("WordCount")
        .setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    Dataset<Row> data = sparkSession.range(100).withColumnRenamed("id", "x");
    UserDefinedFunction squared = udf((Long x) -> x * x, DataTypes.LongType);
    data.withColumn("x²", squared.apply(col("x"))).show();
  }

}
