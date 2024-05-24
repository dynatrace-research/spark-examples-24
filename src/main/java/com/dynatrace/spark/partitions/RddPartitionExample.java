package com.dynatrace.spark.partitions;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RddPartitionExample {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    JavaRDD<Integer> rdd;
    try (JavaSparkContext jContext = new JavaSparkContext(conf)) {
      jContext.sc().setLogLevel("ERROR");

      // used instead of SparkContext.range() to avoid Scala types
      List<Integer> values = IntStream.range(0, 100).boxed().collect(Collectors.toList());
      rdd = jContext.parallelize(values);

      rdd = rdd.filter(x -> x % 3 == 0);

      System.out.println(rdd.collect());
      System.out.println("partitions: " + rdd.getNumPartitions());
    }
  }

}
