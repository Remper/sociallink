package eu.fbk.ict.fm.ml.evaluation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for custom evaluation for specific setups,
 * use this class only if ConfusionMatrix doesn't make sense for your task
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class SimpleEvaluation {
  private static final Logger logger = LoggerFactory.getLogger(SimpleEvaluation.class);
  private int tp = 0, fp = 0, fn = 0;

  public void tp() {
    tp++;
  }

  public void fp() {
    fp++;
  }

  public void fn() {
    fn++;
  }

  public double getPrecision() {
    return (double) tp / (tp+fp);
  }

  public double getRecall() {
    return (double) tp / (tp+fn);
  }

  public double getF1() {
    double precision = getPrecision();
    double recall = getRecall();
    return 2 * (precision * recall) / (precision + recall);
  }

  public void printResult() {
    logger.info(String.format("Precision: %6.2f%%", getPrecision()*100));
    logger.info(String.format("Recall:    %6.2f%%", getRecall()*100));
    logger.info(String.format("F1:        %6.2f%%", getF1()*100));
  }

  public String printOneliner() {
    return String.format("%.4f\t%.4f\t%.4f", getPrecision(), getRecall(), getF1());
  }
}
