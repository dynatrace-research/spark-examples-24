package com.dynatrace.spark.exercise;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Exercise1 {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    JavaSparkContext jContext = new JavaSparkContext(conf);
    jContext.setLogLevel("ERROR");

    List<Integer> items = IntStream.range(0, 100).boxed().collect(Collectors.toList());
    JavaRDD<Integer> rdd = jContext.parallelize(items)
        .filter(x -> x % 5 == 0)
        .map(x -> x * x);
    System.out.println(rdd.collect());

    jContext.close();
  }

}
