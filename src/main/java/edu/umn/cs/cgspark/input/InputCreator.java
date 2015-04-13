package edu.umn.cs.cgspark.input;

import java.io.IOException;
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
    
    int buffer = 100000;
    boolean firstAppend = false;
    for (int j = 0; j < buffer; ++j) {
      Point[] points = new Point[buffer];
      int k = j * buffer;
      int count = 0;
      for (int i = k; i < k + buffer && i < size; ++i) {
        points[count] = new Point(r.nextDouble() * 100000, r.nextDouble() * 100000);
        count++;
      }
      if (count == 0) {
        break;
      }
      System.out.println("DONE Creating the input for size : " + count);
      String fileName =
          "/Users/prashantchaudhary/Documents/workspace/cgspark/input" + size
              + ".txt";
      if (!firstAppend) {
        System.out.println("Writing the input to " + fileName);
        FileIOUtil.writePointArrayToFile(points, fileName, count);
        System.out.println("DONE Writing the input to " + fileName);
        firstAppend = true;
      } else {
        System.out.println("Appending the input to " + fileName);
        FileIOUtil.appendPointArrayToFile(points, fileName, count);
        System.out.println("DONE Appending the input to " + fileName);
      }
    }
  }

  private static void printUsage() {
    System.out
        .println("Please provide the size of the input file to be created.");
  }

}
