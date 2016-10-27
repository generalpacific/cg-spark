package org.apache.cgspark.operations.local;

import org.apache.cgspark.core.DistancePointPair;
import org.apache.cgspark.core.Point;

/**
 * In memory implementation of Closest pair algorithm based on divide and
 * conquer.
 */
public class ClosestPairLocal {

    private ClosestPairLocal() {

    }

    /**
     * In-memory divide and conquer algorithm for closest pair
     */
    public static DistancePointPair closestPair(Point[] a) {
        return closestPair(a, new Point[a.length], 0, a.length - 1);
    }


    private static DistancePointPair closestPair(Point[] a, Point[] tmp, int
            l, int r) {
        if (l >= r) return null;

        int mid = (l + r) >> 1;
        double medianX = a[mid].x();
        DistancePointPair delta1 = closestPair(a, tmp, l, mid);
        DistancePointPair delta2 = closestPair(a, tmp, mid + 1, r);
        DistancePointPair delta;
        if (delta1 == null || delta2 == null) {
            delta = delta1 == null ? delta2 : delta1;
        } else {
            delta = delta1.distance < delta2.distance ? delta1 : delta2;
        }
        int i = l, j = mid + 1, k = l;

        while (i <= mid && j <= r) {
            if (a[i].y() < a[j].y()) {
                tmp[k++] = (Point) a[i++];
            } else {
                tmp[k++] = (Point) a[j++];
            }
        }
        while (i <= mid) {
            tmp[k++] = a[i++];
        }
        while (j <= r) {
            tmp[k++] = a[j++];
        }

        for (i = l; i <= r; i++) {
            a[i] = tmp[i];
        }

        k = l;
        for (i = l; i <= r; i++) {
            if (delta == null || Math.abs(tmp[i].x() - medianX) <= delta
                    .distance) {
                tmp[k++] = tmp[i];
            }
        }

        for (i = l; i < k; i++) {
            for (j = i + 1; j < k; j++) {
                if (delta != null && tmp[j].y() - tmp[i].y() >= delta
                        .distance) {
                    break;
                } else if (delta == null || tmp[i].distanceTo(tmp[j]) < delta
                        .distance) {
                    if (delta == null) {
                        delta = new DistancePointPair();
                    }
                    delta.distance = tmp[i].distanceTo(tmp[j]);
                    delta.first = tmp[i];
                    delta.second = tmp[j];
                }
            }
        }
        return delta;
    }
}
