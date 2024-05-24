package com.dynatrace.spark.dataset;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class DataSetExampleEncoder {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("DataSetExample").setMaster("local[*]")
        .set("spark.driver.bindAddress", "127.0.0.1");
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    Encoder<Record> encoder = Encoders.javaSerialization(Record.class);
    Dataset<Record> dataset = sparkSession.createDataset(rows(), encoder);

    dataset.show();

    for (Record r : (Record[]) dataset.filter((FilterFunction<Record>) wrapper -> wrapper.getC() == 11).collect()) {
      System.out.println(r);
    }
  }

  private static List<Record> rows() {
    return Arrays.asList(
        new Record("A1", "B1", 11),
        new Record("A1", "B1", 12),
        new Record("A3", "B3", 13)
    );
  }

  public static class Record implements Serializable {
    private static final long serialVersionUID = 1L;
    private String a;
    private String b;
    private int c;

    public Record(String a, String b, int c) {
      this.a = a;
      this.b = b;
      this.c = c;
    }

    public String getA() {
      return a;
    }

    public void setA(String a) {
      this.a = a;
    }

    public String getB() {
      return b;
    }

    public void setB(String b) {
      this.b = b;
    }

    public int getC() {
      return c;
    }

    public void setC(int c) {
      this.c = c;
    }

    @Override
    public String toString() {
      return "Record{" +
          "a='" + a + '\'' +
          ", b='" + b + '\'' +
          ", c=" + c +
          '}';
    }
  }
}


