package edu.umn.cs.cgspark.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.umn.cs.cgspark.core.Point;
import edu.umn.cs.cgspark.util.FileIOUtil;

/**
 * Creates input based on the size passed by the user. The input file is created. The points are
 * generated randomly.
 * 
 * @author prashantchaudhary
 *
 */
public final class InputCreator {

  public static void main(String[] arg) throws IOException {
    if (arg.length != 1) {
      printUsage();
      System.exit(-1);
    }
    int size = Integer.parseInt(arg[0]);

    System.out.println("Creating the input for size : " + size);
    Random r = new Random();
    Point[] points = new Point[size];
    for (int i = 0; i < size; ++i) {
      points[i] = new Point(r.nextDouble(), r.nextDouble());
    }
    System.out.println("DONE Creating the input for size : " + size);
    String fileName =
        "/Users/prashantchaudhary/Documents/workspace/cgspark/input" + size
            + ".txt";
    System.out.println("Writing the input to " + fileName);
    FileIOUtil.writePointArrayToFile(points, fileName);
    System.out.println("DONE Writing the input to " + fileName);
  }

  private static void printUsage() {
    System.out
        .println("Please provide the size of the input file to be created.");
  }

}
