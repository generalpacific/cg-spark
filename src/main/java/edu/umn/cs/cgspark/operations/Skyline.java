package edu.umn.cs.cgspark.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.umn.cs.cgspark.core.Point;
import edu.umn.cs.cgspark.function.PointToXCoordinateMapper;
import edu.umn.cs.cgspark.function.StringToPointMapper;
import edu.umn.cs.cgspark.util.FileIOUtil;

/**
 * Operator for calculating the skyline of given points.
 * 
 * @author prashantchaudhary
 *
 */
public class Skyline {

  /**
   * The recursive method of skyline
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
        && !skylineDominate(skyline2[0], skyline1[cutPointForSkyline1]))
      cutPointForSkyline1++;
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

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      printUsage();
      System.exit(-1);
    }
    String inputFile = args[0];
    SparkConf conf = new SparkConf().setAppName("Skyline Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> inputData = sc.textFile(inputFile);
    JavaRDD<Point> pointsData = inputData.map(new StringToPointMapper());

    List<Point> pointsList = pointsData.toArray();

    // Sort points
    pointsData =
        pointsData.sortBy(new PointToXCoordinateMapper(), true, 1).cache();

    pointsList = pointsData.toArray();
    Point[] pointsArray = new Point[pointsList.size()];
    pointsList.toArray(pointsArray);

    // calculate skyline.
    Point[] skyline = skyline(pointsArray, 0, pointsArray.length);
    System.out.println("Saving skylineRDD to output.txt");
    FileIOUtil.writePointArrayToFile(skyline,
        "/Users/prashantchaudhary/Documents/workspace/cgspark/output.txt");
    System.out.println("DONE Saving skylineRDD to output.txt");
    sc.close();
  }

  private static void printUsage() {
    System.out.println("Please provide the input file.");
  }
}
