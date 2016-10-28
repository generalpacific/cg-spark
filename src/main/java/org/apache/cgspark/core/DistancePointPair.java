package org.apache.cgspark.core;

import com.google.common.base.Objects;

public class DistancePointPair {
    public Point first;
    public Point second;
    public double distance;

    public DistancePointPair(Point first, Point second, double distance) {
        this.first = first;
        this.second = second;
        this.distance = distance;
    }

    public DistancePointPair() {
    }

    /**
     * Rearranges first point and second point based on x co-ordiante i.e.
     * sorts by x co-ordinate.
     */
    public void rebase() {
        if (first.x() < second.x()) {
            return;
        }
        Point temp = first;
        first = second;
        second = temp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DistancePointPair that = (DistancePointPair) o;
        return Double.compare(that.distance, distance) == 0 && Objects.equal
                (first, that.first) && Objects.equal(second, that.second);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first, second, distance);
    }

    @Override
    public String toString() {
        return "DistancePointPair{" + "first=" + first + ", second=" + second
                + ", distance=" + distance + '}';
    }
}
