package eu.fbk.ict.fm.ml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Simple scaler for machine learning purposes
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Scaler {
  private static final Logger logger = LoggerFactory.getLogger(Scaler.class);
  public static final int MAX_DEBUG_OUTPUT = 50;

  private double[] stddev = null;
  private double[] mean = null;

  public void fit(List<double[]> features) {
    if (features.size() == 0) {
      throw new IllegalArgumentException("Feature matrix can't be empty");
    }
    mean = new double[features.get(0).length];
    stddev = new double[mean.length];

    //Calculating mean
    for (int i = 0; i < mean.length; i++) {
      mean[i] = 0;
      stddev[i] = 0;
    }
    for (double[] featureVector : features) {
      for (int i = 0; i < mean.length; i++) {
        mean[i] += featureVector[i];
      }
    }
    for (int i = 0; i < mean.length; i++) {
      mean[i] /= features.size();
    }

    //Calculating standard deviation
    for (double[] featureVector : features) {
      for (int i = 0; i < mean.length; i++) {
        stddev[i] += Math.pow(featureVector[i]-mean[i], 2);
      }
    }
    for (int i = 0; i < mean.length; i++) {
      stddev[i] = Math.sqrt(stddev[i]/features.size());
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Fit training set with idx/mean/stddev: ");
      int outputLength = mean.length > MAX_DEBUG_OUTPUT ? MAX_DEBUG_OUTPUT : mean.length;
      for (int i = 0; i < outputLength; i++) {
        logger.debug(String.format("  %d\t%.2f\t%.2f", i, mean[i], stddev[i]));
      }
      if (mean.length > MAX_DEBUG_OUTPUT) {
        logger.debug("  ...");
      }
    }
  }

  public void transform(double[] features) {
    Objects.requireNonNull(stddev);
    Objects.requireNonNull(mean);

    for (int i = 0; i < features.length; i++) {
      features[i] = features[i] - mean[i];
      if (stddev[i] != 0) {
        features[i] = (features[i] - mean[i]) / stddev[i];
      }
    }
  }

  public void transform(Collection<double[]> features) {
    features.forEach(this::transform);
  }
}
