package com.dynatrace.spark.basics;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.upper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RowExample {

  private static final String VALUE = "value";
  private static final String ID = "id";
  private static final String LETTER = "letter";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    Dataset<Row> ds = sparkSession.createDataFrame(rows(), schema());
    Dataset<Row> result = ds
        .select(col(ID), upper(col(VALUE)).as(LETTER))
        .filter(col(LETTER).notEqual("B"))
        .sort(ID);
    result.show();
  }

  private static List<Row> rows() {
    List<String> items = Arrays.asList("a", "b", "c", "d", "e", "A", "B", "C", "D", "E");
    AtomicInteger i = new AtomicInteger();
    return items.stream().map(x -> RowFactory.create(x, i.getAndIncrement())).collect(Collectors.toList());
  }

  private static StructType schema() {
    return DataTypes.createStructType(new StructField[]{
        new StructField(VALUE, DataTypes.StringType, false, Metadata.empty()),
        new StructField(ID, DataTypes.IntegerType, false, Metadata.empty())
    });
  }

}
