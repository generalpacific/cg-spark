package org.apache.cgspark.input.generator;

import java.util.Random;

import org.apache.cgspark.core.Point;
import org.apache.cgspark.core.Rectangle;

public class UniformDistPointGenerator implements PointGenerator {

  private final Rectangle mbr;
  private final Random random;

  public UniformDistPointGenerator(final Rectangle mbr, final Random random) {
    this.mbr = mbr;
    this.random = random;
  }

  public Point generatePoint() {
    final double x = random.nextDouble() * (mbr.r - mbr.l) + mbr.l;
    final double y = random.nextDouble() * (mbr.t - mbr.b) + mbr.b;
    return new Point(x, y);
  }

}
