package edu.umn.cs.cgspark.input;

import java.io.IOException;
import java.util.Random;

import edu.umn.cs.cgspark.core.Point;
import edu.umn.cs.cgspark.core.Rectangle;
import edu.umn.cs.cgspark.util.FileIOUtil;

/**
 * Creates input based on the size passed by the user. The input file is created. The points are
 * generated randomly.
 * 
 * @author prashantchaudhary
 *
 */
public final class InputCreator {

  public final static int mbr_max = 1000000;
  private final static double rho = 0.9;

  public enum DistributionType {
    UNIFORM, GAUSSIAN, CORRELATED, ANTI_CORRELATED, CIRCLE
  }

  public static void main(String[] arg) throws IOException {
    if (arg.length != 3) {
      printUsage();
      System.exit(-1);
    }
    int size = Integer.parseInt(arg[1]);
    DistributionType type = getDistributionType(arg[2]);
    if (type == null) {
      System.out.println("Invalid Distribution type: " + arg[2]);
      System.exit(-1);
    }

    System.out.println("Creating the input for size : " + size);
    System.out.println("Creating the input for type : " + type);
    Random r = new Random(System.currentTimeMillis());
    Rectangle mbr = new Rectangle(0, mbr_max, 0, mbr_max);

    int buffer = 100000;
    boolean firstAppend = false;
    for (int j = 0; j < buffer; ++j) {
      Point[] points = new Point[buffer];
      int k = j * buffer;
      int count = 0;
      for (int i = k; i < k + buffer && i < size; ++i) {
        points[count] = generatePoint(mbr, type, r);
        count++;
      }
      if (count == 0) {
        break;
      }
      System.out.println("DONE Creating the input for size : " + count);
      String fileName = arg[0];
      if (!firstAppend) {
        System.out.println("Writing the input to " + fileName);
        FileIOUtil.writePointArrayToFile(points, fileName, count);
        System.out.println("DONE Writing the input to " + fileName);
        firstAppend = true;
      } else {
        System.out.println("Appending the input to " + fileName);
        FileIOUtil.appendPointArrayToFile(points, fileName, count);
        System.out.println("DONE Appending the input to " + fileName);
      }
    }
  }

  public static Point generatePoint(Rectangle rectangle, DistributionType type,
      Random rand) {
    double x, y;
    switch (type) {
      case UNIFORM:
        x = rand.nextDouble() * (rectangle.r - rectangle.l) + rectangle.l;
        y = rand.nextDouble() * (rectangle.t - rectangle.b) + rectangle.b;
        return new Point(x, y);
      case GAUSSIAN:
        x =
            nextGaussian(rand) * (rectangle.r - rectangle.l) / 2.0
                + (rectangle.l + rectangle.r) / 2.0;
        y =
            nextGaussian(rand) * (rectangle.t - rectangle.b) / 2.0
                + (rectangle.b + rectangle.t) / 2.0;
        return new Point(x, y);
      case CORRELATED:
      case ANTI_CORRELATED:
        x = rand.nextDouble() * 2 - 1;
        do {
          y = rho * x + Math.sqrt(1 - rho * rho) * nextGaussian(rand);
        } while (y < -1 || y > 1);
        x =
            x * (rectangle.r - rectangle.l) / 2.0 + (rectangle.l + rectangle.r)
                / 2.0;
        y =
            y * (rectangle.t - rectangle.b) / 2.0 + (rectangle.b + rectangle.t)
                / 2.0;
        if (type == DistributionType.ANTI_CORRELATED)
          y = rectangle.t - (y - rectangle.b);
        return new Point(x, y);
      case CIRCLE:
        double degree = rand.nextDouble() * Math.PI * 2;
        double xradius;
        do {
          xradius =
              (rectangle.r - rectangle.l) / 2
                  * (0.8 + rand.nextGaussian() / 30);
        } while (xradius > (rectangle.r - rectangle.l) / 2);
        double yradius;
        do {
          yradius =
              (rectangle.t - rectangle.b) / 2
                  * (0.8 + rand.nextGaussian() / 30);
        } while (yradius > (rectangle.t - rectangle.b) / 2);
        double dx = Math.cos(degree) * xradius;
        double dy = Math.sin(degree) * yradius;
        x = (rectangle.l + rectangle.r) / 2 + dx;
        y = (rectangle.b + rectangle.t) / 2 + dy;
        return new Point(x, y);
      default:
        throw new RuntimeException("Unrecognized distribution type: " + type);
    }
  }

  public static DistributionType getDistributionType(String strType) {
    DistributionType type = null;
    if (strType != null) {
      strType = strType.toLowerCase();
      if (strType.startsWith("uni"))
        type = DistributionType.UNIFORM;
      else if (strType.startsWith("gaus"))
        type = DistributionType.GAUSSIAN;
      else if (strType.startsWith("cor"))
        type = DistributionType.CORRELATED;
      else if (strType.startsWith("anti"))
        type = DistributionType.ANTI_CORRELATED;
      else if (strType.startsWith("circle"))
        type = DistributionType.CIRCLE;
      else {
        type = null;
      }
    }
    return type;
  }

  public static double nextGaussian(Random rand) {
    double res = 0;
    do {
      res = rand.nextGaussian() / 5.0;
    } while (res < -1 || res > 1);
    return res;
  }

  private static void printUsage() {
    System.out
        .println("args: <filename> <number of points> <distribution type>");
  }

}
