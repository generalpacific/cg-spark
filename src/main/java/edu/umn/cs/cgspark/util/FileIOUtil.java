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
  
  public static void appendPointArrayToFile(Point[] points, String fileName, int size)
      throws IOException {
    /* BufferedWriter outputWriter = null;
    for (int i = 1; i < 10000000; ++i) {
      outputWriter = new BufferedWriter(new FileWriter(fileName, true));
      int j = i * 10000;
      int k = j;
      for (k = j; k < j + 10000 && k < size; ++k) {
        outputWriter.write(points[k].x() + "," + points[k].y());
        outputWriter.newLine();
      }
      outputWriter.flush();
      outputWriter.close();
      if (k >= size) {
        break;
      }
    } */
    BufferedWriter outputWriter = null;
    outputWriter = new BufferedWriter(new FileWriter(fileName, true));
    for (int i = 0; i < size && i < points.length; ++i) {
      outputWriter.write(points[i].x() + "," + points[i].y());
      outputWriter.newLine();
    }
    outputWriter.flush();
    outputWriter.close();
  }
  
  public static void writePointArrayToFile(Point[] points, String fileName, int size)
      throws IOException {
    /* BufferedWriter outputWriter = null;
    boolean firstAppendDone = false;
    for (int i = 1; i < 10000000; ++i) {
      if (!firstAppendDone) {
        outputWriter = new BufferedWriter(new FileWriter(fileName));
        firstAppendDone = true;
      } else {
        outputWriter = new BufferedWriter(new FileWriter(fileName, true));
      }
      int j = i * 10000;
      int k = j;
      for (k = j; k < j + 10000 && k < size; ++k) {
        outputWriter.write(points[k].x() + "," + points[k].y());
        outputWriter.newLine();
      }
      outputWriter.flush();
      outputWriter.close();
      if (k >= size) {
        break;
      }
    } */
    BufferedWriter outputWriter = null;
    outputWriter = new BufferedWriter(new FileWriter(fileName));
    for (int i = 0; i < size && i < points.length; ++i) {
      outputWriter.write(points[i].x() + "," + points[i].y());
      outputWriter.newLine();
    }
    outputWriter.flush();
    outputWriter.close();
  }
}
