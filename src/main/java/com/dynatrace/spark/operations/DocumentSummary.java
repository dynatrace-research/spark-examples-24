package com.dynatrace.spark.operations;

import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;

public class DocumentSummary {

  /**
   * contains one line of the document together with its length
   */
  public static class LineLength implements Serializable {
    private int lineLength;
    private String text;

    public LineLength() {
    }

    public LineLength(String text, int lineLength) {
      this.text = text;
      this.lineLength = lineLength;
    }

    public int getLineLength() {
      return lineLength;
    }

    public void setLineLength(int lineLength) {
      this.lineLength = lineLength;
    }

    public String getText() {
      return text;
    }

    public void setText(String text) {
      this.text = text;
    }
  }

  /**
   * Aggregation operation to find the longest line
   */
  public static class LineLengthAggregator extends Aggregator<String, LineLength, String> {
    @Override
    public LineLength zero() {
      return new LineLength("", 0);
    }

    @Override
    public LineLength reduce(LineLength a, String b) {
      return a.getLineLength() > b.length() ? a : new LineLength(b, b.length());
    }

    @Override
    public LineLength merge(LineLength a, LineLength b) {
      return a.getLineLength() > b.getLineLength() ? a : b;
    }

    @Override
    public String finish(LineLength reduction) {
      return reduction.getText();
    }

    @Override
    public Encoder<LineLength> bufferEncoder() {
      return Encoders.bean(LineLength.class);
    }

    @Override
    public Encoder<String> outputEncoder() {
      return Encoders.STRING();
    }
  }

  /**
   * Read a document and find the longest line within
   */
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sparkContext = sparkSession.sparkContext();
    sparkContext.setLogLevel("ERROR");

    Dataset<String> lines = sparkSession.read()
        .textFile("./data/loremipsum");
    LineLengthAggregator mostWords = new LineLengthAggregator();
    TypedColumn<String, String> longestLine = mostWords.toColumn().name("Longest line");
    Dataset<String> result = lines.select(longestLine);
    result.show();
  }

}
