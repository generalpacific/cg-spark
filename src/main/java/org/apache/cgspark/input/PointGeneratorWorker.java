package org.apache.cgspark.input;

import java.util.concurrent.Callable;

import org.apache.cgspark.core.Point;
import org.apache.cgspark.input.generator.PointGenerator;
import org.apache.cgspark.util.FileIOUtil;

public final class PointGeneratorWorker implements Callable<Void> {

  private final int numOfPoints;
  private final PointGenerator generator;
  private final boolean firstWrite;
  private final String fileName;

  public PointGeneratorWorker(final int numOfPoints,
      final PointGenerator generator, final boolean firstWrite,
      final String fileName) {
    this.numOfPoints = numOfPoints;
    this.generator = generator;
    this.firstWrite = firstWrite;
    this.fileName = fileName;
  }

  public Void call() throws Exception {
    final Point[] points = new Point[numOfPoints];
    for (int i = 0; i < numOfPoints; ++i) {
      points[i] = generator.generatePoint();
    }
    if (firstWrite) {
      FileIOUtil.writePointArrayToFile(points, fileName);
    } else {
      FileIOUtil.appendPointArrayToFile(points, fileName);
    }
    return null;
  }

}
