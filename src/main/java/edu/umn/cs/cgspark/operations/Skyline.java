package edu.umn.cs.cgspark.operations;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class Skyline {
  public static void main(String[] args) {
    String logFile =
        "/Users/prashantchaudhary/Documents/workspace/cgspark/README.md";
    SparkConf conf = new SparkConf().setAppName("Skyline Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      private static final long serialVersionUID = -782330224486707358L;

      public Boolean call(String s) {
        return s.contains("a");
      }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      private static final long serialVersionUID = 7832587123429873071L;

      public Boolean call(String s) {
        return s.contains("b");
      }
    }).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    sc.close();
  }
}
