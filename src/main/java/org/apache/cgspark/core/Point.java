package org.apache.cgspark.core;

import java.io.Serializable;

/**
 * Class to hold all points in plane.
 *
 * @author prashantchaudhary
 */
public final class Point implements Serializable {
    private static final long serialVersionUID = -1517447420055192047L;
    private double x;
    private double y;

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double x() {
        return x;
    }

    public double y() {
        return y;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(x);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(y);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Point other = (Point) obj;
        if (Double.doubleToLongBits(x) != Double.doubleToLongBits(other.x))
            return false;
        if (Double.doubleToLongBits(y) != Double.doubleToLongBits(other.y))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "(" + x + "," + y + ")";
    }

    public double distanceTo(Point s) {
        double dx = s.x - this.x;
        double dy = s.y - this.y;
        return Math.sqrt(dx * dx + dy * dy);
    }
}
