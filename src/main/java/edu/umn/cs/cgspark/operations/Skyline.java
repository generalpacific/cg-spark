package edu.umn.cs.cgspark.operations;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.umn.cs.cgspark.core.Point;
import edu.umn.cs.cgspark.function.PointToXCoordinateMapper;
import edu.umn.cs.cgspark.function.StringToPointMapper;

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
    int cut_point = 0;
    while (cut_point < skyline1.length
        && !skylineDominate(skyline2[0], skyline1[cut_point]))
      cut_point++;
    Point[] result = new Point[cut_point + skyline2.length];
    System.arraycopy(skyline1, 0, result, 0, cut_point);
    System.arraycopy(skyline2, 0, result, cut_point, skyline2.length);
    return result;
  }

  /**
   * Returns true if p1 dominates p2 according in maxmax
   */
  private static boolean skylineDominate(Point p1, Point p2) {
    return p1.x() >= p2.x() && p1.y() >= p2.y();
  }

  public static void main(String[] args) {
    String inputFile =
        "/Users/prashantchaudhary/Documents/workspace/cgspark/input.txt";
    SparkConf conf = new SparkConf().setAppName("Skyline Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> inputData = sc.textFile(inputFile);
    JavaRDD<Point> pointsData = inputData.map(new StringToPointMapper());

    List<Point> pointsList = pointsData.toArray();
    System.out.println("Read points: " + pointsList);

    // Sort points
    pointsData =
        pointsData.sortBy(new PointToXCoordinateMapper(), true, 1).cache();
    System.out.println("sorted points: " + pointsList);

    pointsList = pointsData.toArray();
    Point[] pointsArray = new Point[pointsList.size()];
    pointsList.toArray(pointsArray);

    // calculate skyline.
    Point[] skyline = skyline(pointsArray, 0, pointsArray.length);
    System.out.println("Skyline: ");
    for (Point p : skyline) {
      System.out.print(p + " ");
    }
    System.out.println();

    sc.close();
  }
}
