package edu.umn.cs.cgspark.function;

import org.apache.spark.api.java.function.Function;

import edu.umn.cs.cgspark.core.Point;

public class PointToXCoordinateMapper implements Function<Point, Double> {

  private static final long serialVersionUID = 7599814167786786973L;

  public Double call(Point p) throws Exception {
    return p.x();
  }

}
