package com.dynatrace.spark.exercise;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Exercise2 {

  private static final String ID = "id";
  private static final String X2 = "x²";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    Dataset<Long> ds = sparkSession.range(100);

    // // alternative solution
    //        ds.createOrReplaceTempView("data");
    //        Dataset<Row> result = sparkSession.sql(
    //                "SELECT id as value, (id * id) as x2 FROM data WHERE id % 3 == 0");
    //        result.show();

    Dataset<Row> result = ds
        .filter(col(ID).mod(3).equalTo(0))
        .select(col(ID), pow(col(ID), 2).cast("int").as(X2));
    result.show(34);
  }

}
