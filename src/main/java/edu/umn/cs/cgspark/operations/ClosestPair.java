package edu.umn.cs.cgspark.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import edu.umn.cs.cgspark.core.DistancePointPair;
import edu.umn.cs.cgspark.core.Point;
import edu.umn.cs.cgspark.core.Rectangle;
import edu.umn.cs.cgspark.function.StringToPointMapper;
import edu.umn.cs.cgspark.function.XCoordinateComparator;
import edu.umn.cs.cgspark.input.InputCreator;
import edu.umn.cs.cgspark.util.FileIOUtil;
import edu.umn.cs.cgspark.util.Util;

/**
 * Operator for calculating the closest pair in given points.
 * 
 * @author prashantchaudhary
 *
 */
public class ClosestPair {

  public static JavaSparkContext sc;
  public static int PARTITIONSIZE = 100;
  
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
    SparkConf conf = new SparkConf().setAppName("ClosestPair Application");
    sc = new JavaSparkContext(conf);

    System.out.println("Creating JavaRDD from file : " + inputFile);
    JavaRDD<String> inputData = sc.textFile(inputFile, 32);
    JavaRDD<Point> pointsData = inputData.map(new StringToPointMapper());
    System.out.println("DONE Creating JavaRDD from file : " + inputFile);

    if (isLocal) {
      Point[] pointsArray = Util.listToArray(pointsData.toArray());
      Arrays.sort(pointsArray, new XCoordinateComparator());
      // calculate closestPair.
      DistancePointPair closestPair =
          closestPair(pointsArray, new Point[pointsArray.length], 0,
              pointsArray.length - 1);
      System.out.println("Saving closestpair to output.txt");
      Point output[] = new Point[2];
      output[0] = closestPair.first;
      output[1] = closestPair.second;
      FileIOUtil.writePointArrayToFile(output, outputFile);
      System.out.println("Closest pair: ");
      System.out.println("Point 1: " + closestPair.first);
      System.out.println("Point 2: " + closestPair.second);
      System.out.println("Distance: " + closestPair.distance);
      System.out.println("DONE Saving closestpair to output.txt");
      System.out.println("Total time taken: "
          + (System.currentTimeMillis() - start) + "ms");
      return;
    }

    /* 
     * Create grid partitions
     */
    System.out.println("Mapping points");

    final int dividerValueX =
        (int) ((InputCreator.mbr_max - 0) / PARTITIONSIZE);

    final int dividerValueY =
        (int) ((InputCreator.mbr_max - 0) / PARTITIONSIZE);

    JavaPairRDD<Rectangle, Point> keyToPointsData =
        pointsData.mapToPair(new PairFunction<Point, Rectangle, Point>() {

          private static final long serialVersionUID = -433072613673987883L;

          public Tuple2<Rectangle, Point> call(Point p) throws Exception {
            int l = ((int) p.x() / dividerValueX) * dividerValueX;
            int r = l + dividerValueX;
            int b = ((int) p.y() / dividerValueY) * dividerValueY;
            int t = b + dividerValueY;
            Rectangle rectangle = new Rectangle(l, r, b, t);
            return new Tuple2<Rectangle, Point>(rectangle, p);
          }
        });
    System.out.println("DONE Mapping points no=" + keyToPointsData.count()
        + " in " + (System.currentTimeMillis() - start));

    long start2 = System.currentTimeMillis();
    System.out.println("Creating partitions from mapped points");
    JavaPairRDD<Rectangle, Iterable<Point>> partitionedPointsRDD =
        keyToPointsData.groupByKey(1000);
    System.out.println("DONE Creating partitions from mapped points: "
        + partitionedPointsRDD.count() + " in "
        + (System.currentTimeMillis() - start2) + "ms");

    /*
     * Filter out candidates for the the final in-memory closest pair
     */
    start2 = System.currentTimeMillis();
    System.out.println("Calculating closestpairs individual partitions.");
    partitionedPointsRDD =
        partitionedPointsRDD
            .mapValues(new Function<Iterable<Point>, Iterable<Point>>() {

              private static final long serialVersionUID = 4592384070663695223L;

              public Iterable<Point> call(Iterable<Point> v1) throws Exception {

                Point[] pointsArray = Util.iterableToArray(v1);
                if (pointsArray == null) {
                  return new ArrayList<Point>();
                }
                Arrays.sort(pointsArray, new XCoordinateComparator());
                // calculate skyline.
                DistancePointPair closestPair =
                    closestPair(pointsArray, new Point[pointsArray.length], 0,
                        pointsArray.length - 1);

                List<Point> candidates = new ArrayList<Point>();
                if (closestPair == null || pointsArray.length == 1) {
                  candidates.add(pointsArray[0]);
                  return candidates;
                }
                candidates.add(closestPair.first);
                candidates.add(closestPair.second);
                for (Point p : pointsArray) {
                  if (p.equals(closestPair.first)
                      || p.equals(closestPair.second)) {
                    continue;
                  }
                  int l = ((int) p.x() / dividerValueX) * dividerValueX;
                  int r = l + dividerValueX;
                  int b = ((int) p.y() / dividerValueY) * dividerValueY;
                  int t = b + dividerValueY;
                  double distance = closestPair.distance;
                  if (p.x() - l <= distance || r - p.x() <= distance) {
                    candidates.add(p);
                    continue;
                  }
                  if (p.y() - b <= distance || t - p.y() <= distance) {
                    candidates.add(p);
                  }
                }

                return candidates;
              }
            });
    // partitionedPointsRDD = partitionedPointsRDD.cache();
    System.out
        .println("DONE Calculating closest pairs for partitions. Number of partitions: "
            + partitionedPointsRDD.count()
            + " in "
            + (System.currentTimeMillis() - start2) + "ms");

    /*
     * Calculate closest pairs from filtered candidates
     */
    start2 = System.currentTimeMillis();
    System.out.println("Calculating closest pairs from candidates.");
    JavaRDD<Iterable<Point>> values = partitionedPointsRDD.values();
    List<Iterable<Point>> array = values.toArray();
    List<Point> finalCandidates = new ArrayList<Point>();
    for (Iterable<Point> points : array) {
      for (Point point : points) {
        finalCandidates.add(point);
      }
    }
    Collections.sort(finalCandidates, new XCoordinateComparator());
    System.out.println("Final candidates size: " + finalCandidates.size());
    Point[] listToArray = Util.listToArray(finalCandidates);
    DistancePointPair closestPair =
        closestPair(listToArray, new Point[listToArray.length], 0,
            listToArray.length - 1);
    System.out.println("DONE Calculating closest pairs from candidates in "
        + (System.currentTimeMillis() - start2) + "ms");
    System.out.println("Closest pair: ");
    System.out.println("Point 1: " + closestPair.first);
    System.out.println("Point 2: " + closestPair.second);
    System.out.println("Distance: " + closestPair.distance);
    System.out.println("Total time taken: "
        + (System.currentTimeMillis() - start) + "ms");
    sc.close();
  }

  private static void printUsage() {
    System.out
        .println("Args: <Inputfile> <Outputfile> <isLocal> <paritionsize>");
  }
  

  /**
   * In-memory divide and conquer algorithm for closest pair
   * 
   * @param a
   * @param tmp
   * @param l
   * @param r
   * @return
   */
  public static DistancePointPair closestPair(Point[] a, Point[] tmp, int l,
      int r) {
    if (l >= r)
      return null;

    int mid = (l + r) >> 1;
    double medianX = a[mid].x();
    DistancePointPair delta1 = closestPair(a, tmp, l, mid);
    DistancePointPair delta2 = closestPair(a, tmp, mid + 1, r);
    DistancePointPair delta;
    if (delta1 == null || delta2 == null) {
      delta = delta1 == null ? delta2 : delta1;
    } else {
      delta = delta1.distance < delta2.distance ? delta1 : delta2;
    }
    int i = l, j = mid + 1, k = l;

    while (i <= mid && j <= r) {
      if (a[i].y() < a[j].y()) {
        tmp[k++] = (Point) a[i++];
      } else {
        tmp[k++] = (Point) a[j++];
      }
    }
    while (i <= mid) {
      tmp[k++] = a[i++];
    }
    while (j <= r) {
      tmp[k++] = a[j++];
    }

    for (i = l; i <= r; i++) {
      a[i] = tmp[i];
    }

    k = l;
    for (i = l; i <= r; i++) {
      if (delta == null || Math.abs(tmp[i].x() - medianX) <= delta.distance) {
        tmp[k++] = tmp[i];
      }
    }

    for (i = l; i < k; i++) {
      for (j = i + 1; j < k; j++) {
        if (delta != null && tmp[j].y() - tmp[i].y() >= delta.distance) {
          break;
        } else if (delta == null || tmp[i].distanceTo(tmp[j]) < delta.distance) {
          if (delta == null) {
            delta = new DistancePointPair();
          }
          delta.distance = tmp[i].distanceTo(tmp[j]);
          delta.first = tmp[i];
          delta.second = tmp[j];
        }
      }
    }
    return delta;
  }
}
