package edu.umn.cs.cgspark.function;

import org.apache.spark.api.java.function.Function;
import edu.umn.cs.cgspark.core.Point;

/**
 * Maps a read input String to a {@link Point}
 * 
 * @author prashantchaudhary
 *
 */
public class StringToPointMapper implements Function<String, Point> {

  private static final long serialVersionUID = -5386741189539453160L;

  public Point call(String inputString) throws Exception {
    if (inputString == null) {
      throw new IllegalArgumentException("Input String cannot be null.");
    }
    if (inputString.isEmpty()) {
      throw new IllegalArgumentException("Input String cannot be empty.");
    }
    // TODO(prashantchaudhary) : do not hard code ",". Preferably add it to a constants file.
    String[] split = inputString.split(",");
    if (split == null || split.length != 2) {
      throw new IllegalArgumentException("Invalid input string in input: "
          + inputString);
    }
    return new Point(Double.parseDouble(split[0]), Double.parseDouble(split[1]));
  }

}
