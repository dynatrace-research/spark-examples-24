package com.dynatrace.spark.operations;

import static org.apache.spark.sql.functions.mean;

import org.apache.hadoop.shaded.com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class AnimalList {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("AnimalList").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sparkContext = sparkSession.sparkContext();
    sparkContext.setLogLevel("ERROR");

    JavaRDD<Animal> rdd = sparkSession
        .read()
        .option("multiline", "true")
        .json("./data/animals.json")
        .toJSON()
        .javaRDD()
        .map(obj -> {
          var gson = new Gson();
          return gson.fromJson(obj, Animal.class);
        });

    Dataset<Animal> animals = sparkSession.createDataset(rdd.rdd(), Encoders.bean(Animal.class));
    animals
        .groupBy("species")
        .agg(mean("age"))
        .filter(animals.col("species").equalTo("CAT"))
        .show();
  }

}
