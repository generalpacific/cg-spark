package org.apache.cgspark.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.cgspark.core.DistancePointPair;
import org.apache.cgspark.core.Point;

/**
 * Util functions to IO in text files
 *
 * @author prashantchaudhary
 */
public final class FileIOUtil {

    private FileIOUtil() {
        throw new IllegalStateException();
    }

    public static synchronized void appendPointArrayToFile(Point[] points,
                                                           String fileName)
            throws IOException {
        BufferedWriter outputWriter = null;
        try {
            outputWriter = new BufferedWriter(new FileWriter(fileName, true));
            for (int i = 0; i < points.length; ++i) {
                outputWriter.write(points[i].x() + "," + points[i].y());
                outputWriter.newLine();
            }
        } finally {
            if (outputWriter != null) {
                outputWriter.flush();
                outputWriter.close();
            }
        }
    }

    public static synchronized void writePointArrayToFile(Point[] points,
                                                          String fileName)
            throws IOException {
        BufferedWriter outputWriter = null;
        try {
            outputWriter = new BufferedWriter(new FileWriter(fileName));
            for (int i = 0; i < points.length; ++i) {
                outputWriter.write(points[i].x() + "," + points[i].y());
                outputWriter.newLine();
            }
        } finally {
            if (outputWriter != null) {
                outputWriter.flush();
                outputWriter.close();
            }
        }
    }

    public static synchronized void writeDistancePointPairArrayToFile
            (DistancePointPair distancePointPair, String fileName) throws
            IOException {
        BufferedWriter outputWriter = null;
        try {
            outputWriter = new BufferedWriter(new FileWriter(fileName));
            outputWriter.write(distancePointPair.first.x() + "," +
                    distancePointPair.first.y());
            outputWriter.newLine();
            outputWriter.write(distancePointPair.second.x() + "," +
                    distancePointPair.second.y());
            outputWriter.newLine();
            outputWriter.write(distancePointPair.distance + "");
            outputWriter.newLine();
        } finally {
            if (outputWriter != null) {
                outputWriter.flush();
                outputWriter.close();
            }
        }
    }
}
