package org.apache.cgspark.function;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.cgspark.core.Point;

public class XCoordinateComparator implements Comparator<Point>, Serializable {

    private static final long serialVersionUID = 214431007811597186L;

    public int compare(Point o1, Point o2) {
        return Double.compare(o1.x(), o2.x());
    }

}
