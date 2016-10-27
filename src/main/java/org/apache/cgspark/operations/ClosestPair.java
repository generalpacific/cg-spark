package org.apache.cgspark.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cgspark.core.DistancePointPair;
import org.apache.cgspark.core.Point;
import org.apache.cgspark.core.Rectangle;
import org.apache.cgspark.function.StringToPointMapper;
import org.apache.cgspark.function.XCoordinateComparator;
import org.apache.cgspark.input.InputCreator;
import org.apache.cgspark.operations.local.ClosestPairLocal;
import org.apache.cgspark.util.FileIOUtil;
import org.apache.cgspark.util.Util;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Operator for calculating the closest pair in given points.
 *
 * @author prashantchaudhary
 */
public class ClosestPair {

    private static Logger logger = LoggerFactory.getLogger(ClosestPair.class);
    public static JavaSparkContext sc;
    public static int PARTITIONSIZE = 100;

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            printUsage();
            System.exit(-1);
        }
        long start = System.currentTimeMillis();
        boolean isLocal = Boolean.parseBoolean(args[2]);
        String inputFile = args[0];
        String outputFile = args[1];
        PARTITIONSIZE = Integer.parseInt(args[3]);
        SparkConf conf = new SparkConf().setAppName("ClosestPair Application");
        sc = new JavaSparkContext(conf);

        logger.info("Creating JavaRDD from file : " + inputFile);
        JavaRDD<String> inputData = sc.textFile(inputFile, 32);
        JavaRDD<Point> pointsData = inputData.map(new StringToPointMapper());
        logger.info("DONE Creating JavaRDD from file : " + inputFile);

        if (isLocal) {
            Point[] pointsArray = Util.listToArray(pointsData.toArray());
            Arrays.sort(pointsArray, new XCoordinateComparator());
            // calculate closestPair.
            DistancePointPair closestPair = ClosestPairLocal.closestPair
                    (pointsArray);
            logger.info("Saving closestpair to output.txt");
            Point output[] = new Point[2];
            output[0] = closestPair.first;
            output[1] = closestPair.second;
            FileIOUtil.writePointArrayToFile(output, outputFile);
            logger.info("Closest pair: ");
            logger.info("Point 1: " + closestPair.first);
            logger.info("Point 2: " + closestPair.second);
            logger.info("Distance: " + closestPair.distance);
            logger.info("DONE Saving closestpair to output.txt");
            logger.info("Total time taken: " + (System.currentTimeMillis() -
                    start) + "ms");
            return;
        }

    /* 
     * Create grid partitions
     */
        logger.info("Mapping points");

        final int dividerValueX = (int) ((InputCreator.mbr_max - 0) /
                PARTITIONSIZE);

        final int dividerValueY = (int) ((InputCreator.mbr_max - 0) /
                PARTITIONSIZE);

        JavaPairRDD<Rectangle, Point> keyToPointsData = pointsData.mapToPair
                (new PairFunction<Point, Rectangle, Point>() {

            private static final long serialVersionUID = -433072613673987883L;

            public Tuple2<Rectangle, Point> call(Point p) throws Exception {
                int l = ((int) p.x() / dividerValueX) * dividerValueX;
                int r = l + dividerValueX;
                int b = ((int) p.y() / dividerValueY) * dividerValueY;
                int t = b + dividerValueY;
                Rectangle rectangle = new Rectangle(l, r, b, t);
                return new Tuple2<Rectangle, Point>(rectangle, p);
            }
        });
        logger.info("DONE Mapping points no=" + keyToPointsData.count() + " " +
                "in " + (System.currentTimeMillis() - start));

        long start2 = System.currentTimeMillis();
        logger.info("Creating partitions from mapped points");
        JavaPairRDD<Rectangle, Iterable<Point>> partitionedPointsRDD =
                keyToPointsData.groupByKey(1000);
        logger.info("DONE Creating partitions from mapped points: " +
                partitionedPointsRDD.count() + " in " + (System
                .currentTimeMillis() - start2) + "ms");

    /*
     * Filter out candidates for the the final in-memory closest pair
     */
        start2 = System.currentTimeMillis();
        logger.info("Calculating closestpairs individual partitions.");
        partitionedPointsRDD = partitionedPointsRDD.mapValues(new Function<Iterable<Point>, Iterable<Point>>() {

            private static final long serialVersionUID = 4592384070663695223L;

            public Iterable<Point> call(Iterable<Point> v1) throws Exception {

                Point[] pointsArray = Util.iterableToArray(v1);
                if (pointsArray == null) {
                    return new ArrayList<Point>();
                }
                Arrays.sort(pointsArray, new XCoordinateComparator());
                // calculate skyline.
                DistancePointPair closestPair = ClosestPairLocal.closestPair
                        (pointsArray);

                List<Point> candidates = new ArrayList<Point>();
                if (closestPair == null || pointsArray.length == 1) {
                    candidates.add(pointsArray[0]);
                    return candidates;
                }
                candidates.add(closestPair.first);
                candidates.add(closestPair.second);
                for (Point p : pointsArray) {
                    if (p.equals(closestPair.first) || p.equals(closestPair
                            .second)) {
                        continue;
                    }
                    int l = ((int) p.x() / dividerValueX) * dividerValueX;
                    int r = l + dividerValueX;
                    int b = ((int) p.y() / dividerValueY) * dividerValueY;
                    int t = b + dividerValueY;
                    double distance = closestPair.distance;
                    if (p.x() - l <= distance || r - p.x() <= distance) {
                        candidates.add(p);
                        continue;
                    }
                    if (p.y() - b <= distance || t - p.y() <= distance) {
                        candidates.add(p);
                    }
                }

                return candidates;
            }
        });
        // partitionedPointsRDD = partitionedPointsRDD.cache();
        logger.info("DONE Calculating closest pairs for partitions. Number of" +
                " partitions: " + partitionedPointsRDD.count() + " in " +
                (System.currentTimeMillis() - start2) + "ms");

    /*
     * Calculate closest pairs from filtered candidates
     */
        start2 = System.currentTimeMillis();
        logger.info("Calculating closest pairs from candidates.");
        JavaRDD<Iterable<Point>> values = partitionedPointsRDD.values();
        List<Iterable<Point>> array = values.toArray();
        List<Point> finalCandidates = new ArrayList<Point>();
        for (Iterable<Point> points : array) {
            for (Point point : points) {
                finalCandidates.add(point);
            }
        }
        Collections.sort(finalCandidates, new XCoordinateComparator());
        logger.info("Final candidates size: " + finalCandidates.size());
        Point[] listToArray = Util.listToArray(finalCandidates);
        DistancePointPair closestPair = ClosestPairLocal.closestPair
                (listToArray);
        logger.info("DONE Calculating closest pairs from candidates in " +
                (System.currentTimeMillis() - start2) + "ms");
        logger.info("Closest pair: ");
        logger.info("Point 1: " + closestPair.first);
        logger.info("Point 2: " + closestPair.second);
        logger.info("Distance: " + closestPair.distance);
        logger.info("Total time taken: " + (System.currentTimeMillis() -
                start) + "ms");
        sc.close();
    }

    private static void printUsage() {
        System.out.println("Args: <Inputfile> <Outputfile> <isLocal> <paritionsize>");
    }
}
