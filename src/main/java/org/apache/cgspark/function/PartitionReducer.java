package org.apache.cgspark.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.cgspark.core.Point;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

public class PartitionReducer implements
    Function2<JavaRDD<Point>, JavaRDD<Point>, JavaRDD<Point>> {

  private static final long serialVersionUID = 1252846659216541901L;

  public JavaRDD<Point> call(JavaRDD<Point> v1, JavaRDD<Point> v2)
      throws Exception {
    List<Point> list = v1.toArray();
    List<Point> list1 = v2.toArray();
    List<Point> result = new ArrayList<Point>();
    result.addAll(list);
    result.addAll(list1);
    return null;
  }

}
