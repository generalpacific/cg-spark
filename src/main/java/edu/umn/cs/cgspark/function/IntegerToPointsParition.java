package edu.umn.cs.cgspark.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import edu.umn.cs.cgspark.core.Point;
import scala.Tuple2;

public class IntegerToPointsParition implements PairFunction<Point, Integer, JavaRDD<Point>> {
  
  private static final long serialVersionUID = -9139114352360374430L;
  private static final int partitionSize = 1000;
  private static Double MIN;
  private static Double MAX;
  private final JavaSparkContext sc;
  private static int dividerValue;
  
  public IntegerToPointsParition (JavaSparkContext sc, Double min, Double max) {
    this.sc = sc;
    this.MIN = min;
    this.MAX = max;
    this.dividerValue = (int) ((MAX - MIN) / partitionSize);
  }

  public Tuple2<Integer, JavaRDD<Point>> call(Point t) throws Exception {
    List<Point> points = new ArrayList<Point>();
    points.add(t);
    return new Tuple2<Integer, JavaRDD<Point>>((int) (t.x() / dividerValue), sc.parallelize(points));
  }

}
