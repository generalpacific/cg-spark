package edu.umn.cs.cgspark.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import edu.umn.cs.cgspark.core.Point;

/**
 * Util functions to IO in text files
 * 
 * @author prashantchaudhary
 *
 */
public final class FileIOUtil {

  private FileIOUtil() {
    throw new IllegalStateException();
  }

  public static void writePointArrayToFile(Point[] points, String fileName)
      throws IOException {
    BufferedWriter outputWriter = null;
    outputWriter = new BufferedWriter(new FileWriter(fileName));
    for (Point p : points) {
      outputWriter.write(p.x() + "," + p.y());
      outputWriter.newLine();
    }
    outputWriter.flush();
    outputWriter.close();
  }
}
