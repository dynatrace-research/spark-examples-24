package com.dynatrace.spark.exercise;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class Exercise8 {

  /**
   * Information on one line of the document
   */
  public static class LineSummary {
    private String firstWord;
    private int lineNumber;
    private int wordCount;

    public LineSummary() {
    }

    public String getFirstWord() {
      return firstWord;
    }

    public void setFirstWord(String firstWord) {
      this.firstWord = firstWord;
    }

    public int getLineNumber() {
      return lineNumber;
    }

    public void setLineNumber(int lineNumber) {
      this.lineNumber = lineNumber;
    }

    public int getWordCount() {
      return wordCount;
    }

    public void setWordCount(int wordCount) {
      this.wordCount = wordCount;
    }
  }

  /**
   * Read a document and find the line with the most words
   */
  public static void main(String[] args) {

    // INITIALIZATION

    SparkConf conf = new SparkConf()
        .setAppName("WordCount")
        .setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    // READ DATASET

    AtomicInteger lineNumber = new AtomicInteger();
    Dataset<LineSummary> summaries = sparkSession.read()
        .textFile("./data/loremipsum")
        .map(
            (MapFunction<String, LineSummary>)
                line -> {
                  // TODO
                  return new LineSummary();
                }, Encoders.bean(LineSummary.class));

    // FIND LINE WITH MOST WORDS
    // TODO
  }

}
