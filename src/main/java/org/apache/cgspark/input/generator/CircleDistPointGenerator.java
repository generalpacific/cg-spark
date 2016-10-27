package org.apache.cgspark.input.generator;

import java.util.Random;

import org.apache.cgspark.core.Point;
import org.apache.cgspark.core.Rectangle;

public class CircleDistPointGenerator implements PointGenerator {
    private final Rectangle mbr;
    private final Random random;

    public CircleDistPointGenerator(final Rectangle mbr, final Random random) {
        this.mbr = mbr;
        this.random = random;
    }

    public Point generatePoint() {
        double degree = random.nextDouble() * Math.PI * 2;
        double xradius;
        do {
            xradius = (mbr.r - mbr.l) / 2 * (0.8 + random.nextGaussian() / 30);
        } while (xradius > (mbr.r - mbr.l) / 2);
        double yradius;
        do {
            yradius = (mbr.t - mbr.b) / 2 * (0.8 + random.nextGaussian() / 30);
        } while (yradius > (mbr.t - mbr.b) / 2);
        double dx = Math.cos(degree) * xradius;
        double dy = Math.sin(degree) * yradius;
        double x = (mbr.l + mbr.r) / 2 + dx;
        double y = (mbr.b + mbr.t) / 2 + dy;
        return new Point(x, y);
    }

}
