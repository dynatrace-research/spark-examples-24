package com.dynatrace.spark.exercise;

import static org.apache.spark.sql.functions.udf;

import com.dynatrace.spark.operations.Animal;
import org.apache.hadoop.shaded.com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

public class Exercise9 {

  private static final String PERCENTAGE = "percentage";
  private static final String NUMBER = "number";
  private static final String COLOR = "color";
  private static final String SPECIES = "species";
  private static final String COUNT = "count";
  private static final String COLOR_GROUP = "color group";

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

    UserDefinedFunction colorGroup = udf(
        (String color) -> {
          // TODO
          return "colorful";
        }
        , DataTypes.StringType);

    // TODO
  }

}
