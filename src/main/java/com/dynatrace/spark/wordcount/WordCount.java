package com.dynatrace.spark.wordcount;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class WordCount {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sparkContext = sparkSession.sparkContext();
    sparkContext.setLogLevel("ERROR");

    File file = new File("./data/loremipsum");
    JavaRDD<String> textFile = sparkContext.textFile(file.getPath(), 1).toJavaRDD();
    JavaRDD<String> wordsFromFile = textFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

    JavaPairRDD<String, Integer> javaPairRDD = wordsFromFile
        .filter(x -> !x.isEmpty())
        .mapToPair(t -> new Tuple2<>(t.toLowerCase(), 1))
        .reduceByKey(Integer::sum);
    List<Tuple2<String, Integer>> ordered = javaPairRDD.takeOrdered(15, new TupleComparator());
    System.out.println(ordered.toString());
  }

  static class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    public int compare(Tuple2<String, Integer> t1,
                       Tuple2<String, Integer> t2) {
      return t2._2.compareTo(t1._2);
    }
  }
}
