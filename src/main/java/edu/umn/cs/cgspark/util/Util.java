package edu.umn.cs.cgspark.util;

import java.util.List;

import com.google.common.collect.Iterables;

import edu.umn.cs.cgspark.core.Point;

/**
 * Util class
 * @author prashantchaudhary
 *
 */
public final class Util {
  
  private Util() {
    throw new IllegalStateException();
  }
  
  public static Point[] listToArray(List<Point> list) {
    Point[] array = new Point[list.size()];
    list.toArray(array);
    return array;
  }
  
  public static Point[] iterableToArray(Iterable<Point> list) {
    return Iterables.toArray(list, Point.class);
  }
}
