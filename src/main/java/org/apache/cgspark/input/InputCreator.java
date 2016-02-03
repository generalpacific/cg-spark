package org.apache.cgspark.input;

import java.io.IOException;
import java.util.Random;

import org.apache.cgspark.core.DistributionType;
import org.apache.cgspark.core.Point;
import org.apache.cgspark.core.Rectangle;
import org.apache.cgspark.input.generator.PointGenerator;
import org.apache.cgspark.input.generator.PointGeneratorFactory;
import org.apache.cgspark.util.FileIOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates input based on the size passed by the user. The input file is created. The points are
 * generated based on the distribution that is input.
 *
 */
public final class InputCreator {

  private static Logger logger = LoggerFactory.getLogger(InputCreator.class);
  private final static double rho = 0.9;

  public final static int mbr_max = 1000000;

  public static void main(String[] arg) throws IOException {
    if (arg.length != 3) {
      printUsage();
      System.exit(-1);
    }
    final int size = Integer.parseInt(arg[1]);
    final DistributionType type = DistributionType.fromName(arg[2]);
    final PointGeneratorFactory factory =
        new PointGeneratorFactory(new Rectangle(0, mbr_max, 0, mbr_max),
            new Random(System.currentTimeMillis()), rho);

    if (type == null) {
      System.out.println("Invalid Distribution type: " + arg[2]);
      System.exit(-1);
    }
    final PointGenerator pointGenerator = factory.getPointGenerator(type);

    logger.info("Creating the input for size : " + size);
    logger.info("Creating the input for type : " + type);

    int buffer = 100000;
    boolean firstAppend = false;
    for (int j = 0; j < buffer; ++j) {
      Point[] points = new Point[buffer];
      int k = j * buffer;
      int count = 0;
      for (int i = k; i < k + buffer && i < size; ++i) {
        points[count] = pointGenerator.generatePoint();
        count++;
      }
      if (count == 0) {
        break;
      }
      logger.info("DONE Creating the input for size : " + count);
      String fileName = arg[0];
      if (!firstAppend) {
        logger.info("Writing the input to " + fileName);
        FileIOUtil.writePointArrayToFile(points, fileName, count);
        logger.info("DONE Writing the input to " + fileName);
        firstAppend = true;
      } else {
        logger.info("Appending the input to " + fileName);
        FileIOUtil.appendPointArrayToFile(points, fileName, count);
        logger.info("DONE Appending the input to " + fileName);
      }
    }
  }

  private static void printUsage() {
    System.out
        .println("args: <filename> <number of points> <distribution type>");
  }

}
