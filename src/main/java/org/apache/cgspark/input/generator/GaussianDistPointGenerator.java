package org.apache.cgspark.input.generator;

import java.util.Random;

import org.apache.cgspark.core.Point;
import org.apache.cgspark.core.Rectangle;

public class GaussianDistPointGenerator implements PointGenerator {
    private final Rectangle mbr;
    private final Random random;

    public GaussianDistPointGenerator(final Rectangle mbr, final Random
            random) {
        this.mbr = mbr;
        this.random = random;
    }

    public Point generatePoint() {
        final double x = nextGaussian(random) * (mbr.r - mbr.l) / 2.0 + (mbr
                .l + mbr.r) / 2.0;
        final double y = nextGaussian(random) * (mbr.t - mbr.b) / 2.0 + (mbr
                .b + mbr.t) / 2.0;
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
