package org.apache.cgspark.local;


import com.clearspring.analytics.util.Lists;

import org.apache.cgspark.core.DistancePointPair;
import org.apache.cgspark.core.Point;
import org.apache.cgspark.operations.local.ClosestPairLocal;
import org.apache.cgspark.util.Util;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ClosestPairLocalTest {

    @Test
    public void testThreePoints() {

        final List<Point> pointList = Lists.newArrayList();
        pointList.add(new Point(0, 0));
        pointList.add(new Point(3, 3));
        pointList.add(new Point(4, 4));

        final Point[] pointsArray = Util.listToArray(pointList);

        // calculate closestPair.
        DistancePointPair closestPair = ClosestPairLocal.closestPair
                (pointsArray);
        assertThat("Incorrect first point in closest pair", closestPair
                .first, is(new Point(3, 3)));
        assertThat("Incorrect second point in closest pair", closestPair
                .second, is(new Point(4, 4)));
    }

}
