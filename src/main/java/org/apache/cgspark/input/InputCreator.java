package org.apache.cgspark.input;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.cgspark.core.DistributionType;
import org.apache.cgspark.core.Rectangle;
import org.apache.cgspark.input.generator.PointGenerator;
import org.apache.cgspark.input.generator.PointGeneratorFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Creates input based on the size passed by the user. The input file is created. The points are
 * generated based on the distribution that is input.
 *
 */
public final class InputCreator {

  private static Logger logger = LoggerFactory.getLogger(InputCreator.class);
  private final static double rho = 0.9;

  public final static int mbr_max = 1000000;

  public static void main(String[] arg) throws IOException,
      InterruptedException, ExecutionException {
    if (arg.length != 3) {
      printUsage();
      System.exit(-1);
    }
    
    // Add a STDOUT logger
    final ConsoleAppender console = new ConsoleAppender();
    final String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    console.setLayout(new PatternLayout(PATTERN)); 
    console.setThreshold(Level.INFO);
    console.activateOptions();
    org.apache.log4j.Logger.getRootLogger().addAppender(console);
    
    final int numOfPoints = Integer.parseInt(arg[1]);
    final DistributionType type = DistributionType.fromName(arg[2]);
    final PointGeneratorFactory factory =
        new PointGeneratorFactory(new Rectangle(0, mbr_max, 0, mbr_max),
            new Random(System.currentTimeMillis()), rho);

    if (type == null) {
      System.out.println("Invalid Distribution type: " + arg[2]);
      System.exit(-1);
    }
    final PointGenerator pointGenerator = factory.getPointGenerator(type);

    logger.info("Creating the input for size : " + numOfPoints);
    logger.info("Creating the input for type : " + type);

    final ListeningExecutorService service =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));

    final int buffer = 100000;
    final String fileName = arg[0];
    final List<ListenableFuture<Void>> tasks = Lists.newArrayList();
    boolean firstWrite = true;
    int pointsRemaining = numOfPoints;
    for (int i = 0; i < numOfPoints; i = i + buffer) {
      final int numOfPointsToWrite = Math.min(pointsRemaining, buffer);
      logger.info("Writing " + numOfPointsToWrite + " to " + fileName);
      if (firstWrite) {
        final ListenableFuture<Void> firstFuture =
            service.submit(new PointGeneratorWorker(numOfPointsToWrite,
                pointGenerator, firstWrite, fileName));
        firstFuture.get();
        firstWrite = false;
      } else {
        tasks.add(service.submit(new PointGeneratorWorker(numOfPointsToWrite,
            pointGenerator, firstWrite, fileName)));

      }
      pointsRemaining = pointsRemaining - numOfPointsToWrite;
    }

    final ListenableFuture<List<Void>> results = Futures.allAsList(tasks);

    Futures.addCallback(results, new FutureCallback<List<Void>>() {

      public void onFailure(Throwable arg0) {
        logger.error("Input creator failed with exception.", arg0);
        service.shutdown();
      }

      public void onSuccess(List<Void> arg0) {
        logger.info("Input creation successful.");
        service.shutdown();
      }
    }, service);
    
    results.get();
  }

  private static void printUsage() {
    System.out
        .println("args: <filename> <number of points> <distribution type>");
  }

}
