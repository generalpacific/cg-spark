package org.apache.cgspark.local;


import com.google.common.collect.ImmutableList;

import com.clearspring.analytics.util.Lists;

import org.apache.cgspark.core.DistancePointPair;
import org.apache.cgspark.core.Point;
import org.apache.cgspark.operations.local.ClosestPairLocal;
import org.apache.cgspark.util.Util;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ClosestPairLocalTest {

    private final static Point POINT00 = new Point(0, 0);
    private final static Point POINT11 = new Point(1, 1);
    private final static Point POINT22 = new Point(2, 2);
    private final static Point POINT33 = new Point(3, 3);

    private final static List<Point> TEST_POINTS = ImmutableList.of(new Point
            (0.748501, 4.09624), new Point(3.00302, 5.26164), new Point
            (3.61878, 9.52232), new Point(7.46911, 4.71611), new Point
            (5.7819, 2.69367), new Point(2.34709, 8.74782), new Point
            (2.87169, 5.97774), new Point(6.33101, 0.463131), new Point
            (7.46489, 4.6268), new Point(1.45428, 0.087596));

    @Test
    public void testThreePoints() {

        final List<Point> pointList = Lists.newArrayList();

        pointList.add(POINT00);
        pointList.add(POINT22);
        pointList.add(POINT33);

        final Point[] pointsArray = Util.listToArray(pointList);

        // calculate closestPair.
        final DistancePointPair closestPair = ClosestPairLocal.closestPair
                (pointsArray);
        assertThat("Incorrect first point in closest pair", closestPair
                .first, is(POINT22));
        assertThat("Incorrect second point in closest pair", closestPair
                .second, is(POINT33));
        assertThat("Incorrect distance", closestPair.distance, is
                (POINT22.distanceTo(POINT33)));
    }


    @Test
    public void testTwoPoints() {
        final List<Point> pointList = Lists.newArrayList();
        pointList.add(POINT00);
        pointList.add(POINT11);

        final Point[] pointsArray = Util.listToArray(pointList);

        // calculate closestPair.
        final DistancePointPair closestPair = ClosestPairLocal.closestPair
                (pointsArray);
        assertThat("Incorrect first point in closest pair", closestPair
                .first, is(POINT00));
        assertThat("Incorrect second point in closest pair", closestPair
                .second, is(POINT11));
        assertThat("Incorrect distance", closestPair.distance, is
                (POINT00.distanceTo(POINT11)));
    }

    @Test
    public void testClosestPairTestData() {
        final DistancePointPair closestPair = ClosestPairLocal.closestPair
                (Util.listToArray(TEST_POINTS));
        final Point expectedFirstPoint = TEST_POINTS.get(8);
        assertThat("Incorrect first point in closest pair", closestPair
                .first, is(expectedFirstPoint));
        final Point expectedSecondPoint = TEST_POINTS.get(3);
        assertThat("Incorrect second point in closest pair", closestPair
                .second, is(expectedSecondPoint));
        assertThat("Incorrect distance", closestPair.distance, is
                (expectedFirstPoint.distanceTo(expectedSecondPoint)));
    }

    @Test
    public void testThreePointsBruteForce() {

        final List<Point> pointList = Lists.newArrayList();

        pointList.add(POINT00);
        pointList.add(POINT22);
        pointList.add(POINT33);

        final DistancePointPair closestPair = ClosestPairLocal
                .bruteForceClosestPair(pointList);
        assertThat("Incorrect first point in closest pair", closestPair
                .first, is(POINT22));
        assertThat("Incorrect second point in closest pair", closestPair
                .second, is(POINT33));
        assertThat("Incorrect distance", closestPair.distance, is
                (POINT22.distanceTo(POINT33)));
    }


    @Test
    public void testTwoPointsBruteForce() {
        final List<Point> pointList = Lists.newArrayList();
        pointList.add(POINT00);
        pointList.add(POINT11);

        final DistancePointPair closestPair = ClosestPairLocal
                .bruteForceClosestPair(pointList);
        assertThat("Incorrect first point in closest pair", closestPair
                .first, is(POINT00));
        assertThat("Incorrect second point in closest pair", closestPair
                .second, is(POINT11));
        assertThat("Incorrect distance", closestPair.distance, is
                (POINT00.distanceTo(POINT11)));
    }

    @Test
    public void testClosestPairTestDataBruteForce() {
        final DistancePointPair closestPair = ClosestPairLocal
                .bruteForceClosestPair(TEST_POINTS);
        final Point expectedFirstPoint = TEST_POINTS.get(8);
        assertThat("Incorrect first point in closest pair", closestPair
                .first, is(expectedFirstPoint));
        final Point expectedSecondPoint = TEST_POINTS.get(3);
        assertThat("Incorrect second point in closest pair", closestPair
                .second, is(expectedSecondPoint));
        assertThat("Incorrect distance", closestPair.distance, is
                (expectedFirstPoint.distanceTo(expectedSecondPoint)));
    }
}
