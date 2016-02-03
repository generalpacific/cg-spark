package org.apache.cgspark.function;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.cgspark.core.Point;

public class YCoordinateComparator implements Comparator<Point>, Serializable {

  private static final long serialVersionUID = -1907594718635475297L;

  public int compare(Point o1, Point o2) {
    return Double.compare(o1.y(), o2.y());
  }

}
