package edu.umn.cs.cgspark.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import edu.umn.cs.cgspark.core.Point;
import edu.umn.cs.cgspark.function.StringToPointMapper;
import edu.umn.cs.cgspark.function.XCoordinateComparator;
import edu.umn.cs.cgspark.input.InputCreator;
import edu.umn.cs.cgspark.util.FileIOUtil;
import edu.umn.cs.cgspark.util.Util;

/**
 * Operator for calculating the skyline of given points.
 * 
 * @author prashantchaudhary
 *
 */
public class Skyline {

  public static JavaSparkContext sc;
  public static int PARTITIONSIZE = 10000;
  
  public static void main(String[] args) throws IOException {
    if (args.length != 4) {
      printUsage();
      System.exit(-1);
    }
    long start = System.currentTimeMillis();
    boolean isLocal = Boolean.parseBoolean(args[2]);
    String inputFile = args[0];
    String outputFile = args[1];
    PARTITIONSIZE = Integer.parseInt(args[3]);
    SparkConf conf = new SparkConf().setAppName("Skyline Application");
    sc = new JavaSparkContext(conf);

    System.out.println("Creating JavaRDD from file : " + inputFile);
    JavaRDD<String> inputData = sc.textFile(inputFile, 32);
    JavaRDD<Point> pointsData = inputData.map(new StringToPointMapper());
    System.out.println("DONE Creating JavaRDD from file : " + inputFile);

    if (isLocal) {
      Point[] pointsArray = Util.listToArray(pointsData.toArray());

      // calculate local skyline.
      Point[] skyline = skyline(pointsArray, 0, pointsArray.length);
      System.out.println("Saving skylineRDD to output.txt");
      FileIOUtil.writePointArrayToFile(skyline, outputFile);
      System.out.println("DONE Saving skylineRDD to output.txt");
      return;
    }

    
    /*
     * Create vertical partitions
     */
    System.out.println("Mapping points");

    final int dividerValue = (int) ((InputCreator.mbr_max - 0) / PARTITIONSIZE);
    System.out.println("Divider value: " + dividerValue);

    JavaPairRDD<Integer, Point> keyToPointsData =
        pointsData.mapToPair(new PairFunction<Point, Integer, Point>() {

          private static final long serialVersionUID = -433072613673987883L;

          public Tuple2<Integer, Point> call(Point t) throws Exception {
            return new Tuple2<Integer, Point>((int) (t.x() / dividerValue), t);
          }
        });
    System.out.println("DONE Mapping points: " + keyToPointsData.count()
        + " in " + (System.currentTimeMillis() - start) + "ms");


    long start2 = System.currentTimeMillis();
    System.out.println("Creating partitions from mapped points");
    JavaPairRDD<Integer, Iterable<Point>> partitionedPointsRDD =
        keyToPointsData.groupByKey(1000);
    System.out.println("DONE Creating partitions from mapped points: "
        + partitionedPointsRDD.count() + " in "
        + (System.currentTimeMillis() - start2) + "ms");

    /*
     * Calculate individual skylines
     */
    start2 = System.currentTimeMillis();
    System.out.println("Calculating skylines individual partitions.");
    partitionedPointsRDD =
        partitionedPointsRDD
            .mapValues(new Function<Iterable<Point>, Iterable<Point>>() {

              private static final long serialVersionUID = 4592384070663695223L;

              public Iterable<Point> call(Iterable<Point> v1) throws Exception {
                Point[] pointsArray = Util.iterableToArray(v1);
                // calculate skyline.
                Arrays.sort(pointsArray, new XCoordinateComparator());
                Point[] skyline = skyline(pointsArray, 0, pointsArray.length);
                return Arrays.asList(skyline);
              }
            });
    partitionedPointsRDD = partitionedPointsRDD.cache();
    System.out
        .println("DONE Calculating skylines individual partitions. Number of partitions: "
            + partitionedPointsRDD.count()
            + " in "
            + (System.currentTimeMillis() - start2) + "ms");

    /*
     * Merge all the skylines
     */
    start2 = System.currentTimeMillis();
    System.out.println("Merging individual skylines.");
    partitionedPointsRDD = partitionedPointsRDD.sortByKey(true);
    List<Tuple2<Integer, Iterable<Point>>> skylineTuples =
        partitionedPointsRDD.collect();
    Point[] skyline = Util.iterableToArray(skylineTuples.get(0)._2);
    List<Point> result = new ArrayList<Point>();
    result.addAll(Arrays.asList(skyline));
    for (int i = 1; i < skylineTuples.size(); ++i) {
      Point[] resultArray = Util.listToArray(result);
      Point[] newArray = Util.iterableToArray(skylineTuples.get(i)._2);
      Point[] mergeSkylines = mergeSkylines(resultArray, newArray);
      result.clear();
      result.addAll(Arrays.asList(mergeSkylines));
    }
    System.out.println("DONE Merging individual skylines. in "
        + (System.currentTimeMillis() - start2) + "ms");
    System.out.println("Saving skylineRDD to output.txt");
    FileIOUtil.writePointArrayToFile(Util.listToArray(result), outputFile);
    System.out.println("DONE Saving skylineRDD to output.txt");
    System.out.println("Total time = " + (System.currentTimeMillis() - start)
        + "ms");
    sc.close();
  }

  private static void printUsage() {
    System.out
        .println("Args: <Inputfile> <Outputfile> <isLocal> <PartitionSize>");
  }

  /**
   * The recursive divide and conquer method of skyline
   */
  private static Point[] skyline(Point[] points, int start, int end) {
    if (end - start == 1) {
      // Return the one input point as the skyline
      return new Point[] {points[start]};
    }
    int mid = (start + end) / 2;
    // Find the skyline of each half
    Point[] skyline1 = skyline(points, start, mid);
    Point[] skyline2 = skyline(points, mid, end);
    // Merge the two skylines
    int cutPointForSkyline1 = 0;
    while (cutPointForSkyline1 < skyline1.length
        && !skylineDominate(skyline2[0], skyline1[cutPointForSkyline1])) {
      cutPointForSkyline1++;
    }
    Point[] result = new Point[cutPointForSkyline1 + skyline2.length];
    System.arraycopy(skyline1, 0, result, 0, cutPointForSkyline1);
    System.arraycopy(skyline2, 0, result, cutPointForSkyline1, skyline2.length);
    return result;
  }

  public static Point[] mergeSkylines(Point[] skyline1, Point[] skyline2) {
    int cutPointForSkyline1 = 0;
    while (cutPointForSkyline1 < skyline1.length
        && !skylineDominate(skyline2[0], skyline1[cutPointForSkyline1])) {
      cutPointForSkyline1++;
    }
    Point[] result = new Point[cutPointForSkyline1 + skyline2.length];
    System.arraycopy(skyline1, 0, result, 0, cutPointForSkyline1);
    System.arraycopy(skyline2, 0, result, cutPointForSkyline1, skyline2.length);
    return result;
  }

  /**
   * Returns true if p1 dominates p2 according in maxmax
   */
  private static boolean skylineDominate(Point p1, Point p2) {
    return p1.x() >= p2.x() && p1.y() >= p2.y();
  }
}
