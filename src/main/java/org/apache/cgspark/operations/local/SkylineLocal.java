package org.apache.cgspark.operations.local;

import org.apache.cgspark.core.Point;

public final class SkylineLocal {

    private SkylineLocal() {

    }

    /**
     * The recursive divide and conquer method of skyline
     */
    public static Point[] skyline(Point[] points, int start, int end) {
        if (end - start == 1) {
            // Return the one input point as the skyline
            return new Point[]{points[start]};
        }
        int mid = (start + end) / 2;
        // Find the skyline of each half
        Point[] skyline1 = skyline(points, start, mid);
        Point[] skyline2 = skyline(points, mid, end);
        // Merge the two skylines
        int cutPointForSkyline1 = 0;
        while (cutPointForSkyline1 < skyline1.length && !skylineDominate
                (skyline2[0], skyline1[cutPointForSkyline1])) {
            cutPointForSkyline1++;
        }
        Point[] result = new Point[cutPointForSkyline1 + skyline2.length];
        System.arraycopy(skyline1, 0, result, 0, cutPointForSkyline1);
        System.arraycopy(skyline2, 0, result, cutPointForSkyline1,
                skyline2.length);
        return result;
    }

    public static Point[] mergeSkylines(Point[] skyline1, Point[] skyline2) {
        int cutPointForSkyline1 = 0;
        while (cutPointForSkyline1 < skyline1.length && !skylineDominate
                (skyline2[0], skyline1[cutPointForSkyline1])) {
            cutPointForSkyline1++;
        }
        Point[] result = new Point[cutPointForSkyline1 + skyline2.length];
        System.arraycopy(skyline1, 0, result, 0, cutPointForSkyline1);
        System.arraycopy(skyline2, 0, result, cutPointForSkyline1,
                skyline2.length);
        return result;
    }

    /**
     * Returns true if p1 dominates p2 according in maxmax
     */
    private static boolean skylineDominate(Point p1, Point p2) {
        return p1.x() >= p2.x() && p1.y() >= p2.y();
    }
}
