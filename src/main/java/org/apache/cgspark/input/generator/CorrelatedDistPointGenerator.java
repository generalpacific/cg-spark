package org.apache.cgspark.input.generator;

import java.util.Random;

import org.apache.cgspark.core.Point;
import org.apache.cgspark.core.Rectangle;

public class CorrelatedDistPointGenerator implements PointGenerator {
  private final Rectangle mbr;
  private final Random random;
  private final double rho;

  public CorrelatedDistPointGenerator(final Rectangle mbr, final Random random,
      final double rho) {
    this.mbr = mbr;
    this.random = random;
    this.rho = rho;
  }

  public Point generatePoint() {
    double x = random.nextDouble() * 2 - 1;
    double y;
    do {
      y = rho * x + Math.sqrt(1 - rho * rho) * nextGaussian(random);
    } while (y < -1 || y > 1);
    x = x * (mbr.r - mbr.l) / 2.0 + (mbr.l + mbr.r) / 2.0;
    y = y * (mbr.t - mbr.b) / 2.0 + (mbr.b + mbr.t) / 2.0;
    return new Point(x, y);
  }

  public static double nextGaussian(Random rand) {
    double res = 0;
    do {
      res = rand.nextGaussian() / 5.0;
    } while (res < -1 || res > 1);
    return res;
  }

}
