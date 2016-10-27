package org.apache.cgspark.function;

import org.apache.cgspark.core.Point;
import org.apache.spark.api.java.function.Function;

public class PointToXCoordinateMapper implements Function<Point, Double> {

    private static final long serialVersionUID = 7599814167786786973L;

    public Double call(Point p) throws Exception {
        return p.x();
    }

}
