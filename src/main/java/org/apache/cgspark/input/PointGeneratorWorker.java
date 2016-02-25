package org.apache.cgspark.input;

import java.util.concurrent.Callable;

import org.apache.cgspark.core.Point;
import org.apache.cgspark.input.generator.PointGenerator;
import org.apache.cgspark.util.FileIOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PointGeneratorWorker implements Callable<Void> {

  private static Logger logger = LoggerFactory
      .getLogger(PointGeneratorWorker.class);
  
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
    logger.info("Generating and writing " + numOfPoints + " points to "
        + fileName);
    final Point[] points = new Point[numOfPoints];
    for (int i = 0; i < numOfPoints; ++i) {
      points[i] = generator.generatePoint();
    }
    if (firstWrite) {
      FileIOUtil.writePointArrayToFile(points, fileName);
    } else {
      FileIOUtil.appendPointArrayToFile(points, fileName);
    }
    logger.info("DONE Generating and writing " + numOfPoints + " points to "
        + fileName);
    return null;
  }

}
