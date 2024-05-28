package com.dynatrace.spark.exercise;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;

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
   * Format for the final output
   */
  public static class WordsInLine {
    private int lineNumber;
    private String firstWord;

    public WordsInLine() {
    }

    public WordsInLine(int lineNumber, String firstWord) {
      this.lineNumber = lineNumber;
      this.firstWord = firstWord;
    }

    public int getLineNumber() {
      return lineNumber;
    }

    public void setLineNumber(int lineNumber) {
      this.lineNumber = lineNumber;
    }

    public String getFirstWord() {
      return firstWord;
    }

    public void setFirstWord(String firstWord) {
      this.firstWord = firstWord;
    }
  }

  /**
   * Aggregation operation to find the line with most words
   */
  public static class MostWordsAggregator extends Aggregator<LineSummary, LineSummary, WordsInLine> {
    @Override
    public LineSummary zero() {
      return new LineSummary();
    }

    @Override
    public LineSummary reduce(LineSummary a, LineSummary b) {
      return a.getWordCount() > b.getWordCount() ? a : b;
    }

    @Override
    public LineSummary merge(LineSummary a, LineSummary b) {
      return a.getWordCount() > b.getWordCount() ? a : b;
    }

    @Override
    public WordsInLine finish(LineSummary reduction) {
      return new WordsInLine(reduction.lineNumber, reduction.firstWord);
    }

    @Override
    public Encoder<LineSummary> bufferEncoder() {
      return Encoders.bean(LineSummary.class);
    }

    @Override
    public Encoder<WordsInLine> outputEncoder() {
      return Encoders.bean(WordsInLine.class);
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
                  LineSummary summary = new LineSummary();
                  summary.setLineNumber(lineNumber.getAndIncrement());
                  String[] words = line.split(" ");
                  summary.setWordCount(words.length);
                  summary.setFirstWord(words[0].toLowerCase());
                  return summary;
                }, Encoders.bean(LineSummary.class));

    // FIND LINE WITH MOST WORDS

    MostWordsAggregator mostWords = new MostWordsAggregator();
    TypedColumn<LineSummary, WordsInLine> line = mostWords.toColumn();
    Dataset<WordsInLine> result = summaries.select(line);
    result.show();
  }

}
