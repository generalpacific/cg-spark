package edu.umn.cs.cgspark.core;

public class DistancePointPair {
  public Point first;
  public Point second;
  public double distance;
  
  public DistancePointPair(Point first, Point second, double distance) {
    this.first = first;
    this.second = second;
    this.distance = distance;
  }
  
  public DistancePointPair() {}
}
