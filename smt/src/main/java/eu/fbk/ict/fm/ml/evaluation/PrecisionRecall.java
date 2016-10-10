package eu.fbk.ict.fm.ml.evaluation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import java.io.Serializable;
import java.util.Objects;

/**
 * Computes precision and recall
 *
 * @author Alessio and Francesco
 */
public final class PrecisionRecall implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final PrecisionRecall EMPTY = new PrecisionRecall(0.0, 0.0, 0.0, 0.0,
      Double.NaN, Double.NaN, Double.NaN);

  private final double tp;

  private final double fp;

  private final double fn;

  private final double tn;

  private final double precision;

  private final double recall;

  private final double accuracy;

  private PrecisionRecall(final double tp, final double fp, final double fn, final double tn,
                          final double precision, final double recall, final double accuracy) {
    this.tp = tp;
    this.fp = fp;
    this.fn = fn;
    this.tn = tn;
    this.precision = precision;
    this.recall = recall;
    this.accuracy = accuracy;
  }

  public static PrecisionRecall forCounts(final double tp, final double fp, final double fn) {
    return forCounts(tp, fp, fn, 0.0);
  }

  public static PrecisionRecall forCounts(final double tp, final double fp, final double fn,
                                          final double tn) {
    if (tp < 0.0 || fp < 0.0 || fn < 0.0 || tn < 0.0) {
      throw new IllegalArgumentException("Invalid contingency table values: tp=" + tp
          + ", fp=" + fp + ", fn=" + fn + ", tn=" + tn);
    } else if (tp == 0.0 && fp == 0.0 && fn == 0.0 && tn == 0.0) {
      return EMPTY;
    } else {
      final double c = tp + fp + fn + tn;
      final double a = c == 0.0 ? Double.NaN : (tp + tn) / c;
      final double p = tp + fp == 0.0 ? Double.NaN : tp / (tp + fp);
      final double r = tp + fn == 0.0 ? Double.NaN : tp / (tp + fn);
      return new PrecisionRecall(tp, fp, fn, tn, p, r, a);
    }
  }

  public static PrecisionRecall forMeasures(final double precision, final double recall,
                                            final double accuracy) {
    // Do not estimate TP, FP, FN, TN
    return forMeasures(precision, recall, accuracy, Double.NaN);
  }

  /**
   * Builds a {@code PrecisionRecall} object starting from the supplied precision p, recall r,
   * accuracy a, and count value. The method tries to recover the missing TP, FP, FN and TN
   * values, assigning them the value NaN where recovery is not possible.
   *
   * @param precision
   *            the precision value
   * @param recall
   *            the recall value
   * @param accuracy
   *            the accuracy value
   * @param count
   *            the count value
   * @return the corresponding {@code PrecisionRecall} object
   */
  public static PrecisionRecall forMeasures(final double precision, final double recall,
                                            final double accuracy, final double count) {
    Preconditions.checkArgument(Double.isNaN(precision) || precision >= 0.0
        && precision <= 1.0);
    Preconditions.checkArgument(Double.isNaN(recall) || recall >= 0.0 && recall <= 1.0);
    Preconditions.checkArgument(Double.isNaN(accuracy) || accuracy >= 0.0 && accuracy <= 1.0);
    Preconditions.checkArgument(Double.isNaN(count) || count >= 0.0);
    if (count == Double.NaN || precision == 0.0 || recall == 0.0) {
      return new PrecisionRecall(Double.NaN, Double.NaN, Double.NaN, Double.NaN, precision,
          recall, accuracy);
    } else if (precision == 1.0 && recall == 1.0 || accuracy == 1.0) {
      return new PrecisionRecall(Double.NaN, 0.0, 0.0, Double.NaN, precision, recall,
          accuracy);
    } else {
      final double tp = count * (1 - accuracy) / (1 / precision + 1 / recall - 2);
      final double fp = tp * (1 / precision - 1);
      final double fn = tp * (1 / recall - 1);
      final double tn = count - tp - fp - fn;
      if (tn > 0) {
        return new PrecisionRecall(tp, fp, fn, tn, precision, recall, accuracy);
      }
    }
    return new PrecisionRecall(Double.NaN, Double.NaN, Double.NaN, Double.NaN, precision,
        recall, accuracy);
  }

  public static PrecisionRecall microAverage(final Iterable<PrecisionRecall> prs) {
    double tp = 0.0;
    double fp = 0.0;
    double fn = 0.0;
    double tn = 0.0;
    int num = 0;
    for (final PrecisionRecall pr : prs) {
      tp += pr.tp;
      fp += pr.fp;
      fn += pr.fn;
      tn += pr.tn;
      ++num;
    }
    return num == 0 ? EMPTY : forCounts(tp / num, fp / num, fn / num, tn / num);
  }

  public static PrecisionRecall macroAverage(final Iterable<PrecisionRecall> prs) {
    double p = 0.0;
    double r = 0.0;
    double a = 0.0;
    double count = 0.0;
    int num = 0;
    for (final PrecisionRecall pr : prs) {
      p += pr.getPrecision();
      r += pr.getRecall();
      a += pr.getAccuracy();
      count += pr.getCount();
      ++num;
    }
    return forMeasures(p, r, a, num == 0 ? 0.0 : count / num);
  }

  public double getTP() {
    return this.tp;
  }

  public double getFP() {
    return this.fp;
  }

  public double getFN() {
    return this.fn;
  }

  public double getTN() {
    return this.tn;
  }

  public double getCount() {
    return this.tp + this.fp + this.tn + this.fn;
  }

  public double getAccuracy() {
    return this.accuracy;
  }

  public double getPrecision() {
    return this.precision;
  }

  public double getRecall() {
    return this.recall;
  }

  public double getF1() {
    return this.precision + this.recall == 0.0 ? Double.NaN : 2 * this.precision * this.recall
        / (this.precision + this.recall);
  }

  public double get(final Measure measure) {
    switch (measure) {
      case ACCURACY:
        return getAccuracy();
      case PRECISION:
        return getPrecision();
      case RECALL:
        return getRecall();
      case F1:
        return getF1();
      default:
        throw new IllegalArgumentException("Invalid measure " + measure);
    }
  }

  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (!(object instanceof PrecisionRecall)) {
      return false;
    }
    final PrecisionRecall other = (PrecisionRecall) object;
    return this.tp == other.tp && this.fp == other.fp && this.tn == other.tn
        && this.fn == other.fn && this.precision == other.precision
        && this.recall == other.recall && this.accuracy == other.accuracy;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.tp, this.fp, this.tn, this.fn, this.precision, this.recall,
        this.accuracy);
  }

  @Override
  public String toString() {
    return String.format("p=%.3f r=%.3f f1=%.3f a=%.3f tp=%.2f fp=%.2f fn=%.2f tn=%.2f",
        this.precision, this.recall, getF1(), this.accuracy, this.tp, this.fp, this.fn,
        this.tn);
  }

  public static Ordering<PrecisionRecall> comparator(final Measure measure,
                                                     final boolean higherIsBetter) {
    return new Ordering<PrecisionRecall>() {

      @Override
      public int compare(final PrecisionRecall left, final PrecisionRecall right) {
        final double leftValue = left.get(measure);
        final double rightValue = right.get(measure);
        final int result = Double.compare(leftValue, rightValue);
        return higherIsBetter ? -result : result;
      }

    };
  }

  public static Evaluator evaluator() {
    return new Evaluator();
  }

  public enum Measure {

    ACCURACY,

    PRECISION,

    RECALL,

    F1

  }

  public static final class Evaluator {

    private double tp;

    private double fp;

    private double fn;

    private double tn;

    private PrecisionRecall pr;

    private Evaluator() {
      this.tp = 0.0;
      this.fp = 0.0;
      this.fn = 0.0;
      this.tn = 0.0;
      this.pr = null;
    }

    private static void checkNotNegative(final double value) {
      if (value < 0.0) {
        throw new IllegalArgumentException("Illegal negative value " + value);
      }
    }

    public Evaluator add(final PrecisionRecall pr) {
      if (pr.getCount() > 0) {
        synchronized (this) {
          this.pr = null;
          this.tp += pr.getTP();
          this.fp += pr.getFP();
          this.fn += pr.getFN();
          this.tn += pr.getTN();
        }
      }
      return this;
    }

    public Evaluator add(final PrecisionRecall.Evaluator evaluator) {
      synchronized (evaluator) {
        if (evaluator.tp > 0.0 || evaluator.fp > 0.0 || evaluator.fn > 0.0
            || evaluator.tn > 0.0) {
          synchronized (this) {
            this.pr = null;
            this.tp += evaluator.tp;
            this.fp += evaluator.fp;
            this.fn += evaluator.fn;
            this.tn += evaluator.tn;
          }
        }
      }
      return this;
    }

    public Evaluator add(final double deltaTP, final double deltaFP, final double deltaFN) {
      checkNotNegative(deltaTP);
      checkNotNegative(deltaFP);
      checkNotNegative(deltaFN);
      if (deltaTP > 0.0 || deltaFP > 0.0 || deltaFN > 0.0) {
        synchronized (this) {
          this.pr = null;
          this.tp += deltaTP;
          this.fp += deltaFP;
          this.fn += deltaFN;
        }
      }
      return this;
    }

    public Evaluator add(final double deltaTP, final double deltaFP, final double deltaFN,
                         final double deltaTN) {
      checkNotNegative(deltaTP);
      checkNotNegative(deltaFP);
      checkNotNegative(deltaFN);
      checkNotNegative(deltaTN);
      if (deltaTP > 0.0 || deltaFP > 0.0 || deltaFN > 0.0 || deltaTN > 0.0) {
        synchronized (this) {
          this.pr = null;
          this.tp += deltaTP;
          this.fp += deltaFP;
          this.fn += deltaFN;
          this.tn += deltaTN;
        }
      }
      return this;
    }

    public Evaluator addTP(final double deltaTP) {
      checkNotNegative(deltaTP);
      if (deltaTP != 0.0) {
        synchronized (this) {
          this.pr = null;
          this.tp += deltaTP;
        }
      }
      return this;
    }

    public Evaluator addFP(final double deltaFP) {
      checkNotNegative(deltaFP);
      if (deltaFP != 0.0) {
        synchronized (this) {
          this.pr = null;
          this.fp += deltaFP;
        }
      }
      return this;
    }

    public Evaluator addFN(final double deltaFN) {
      checkNotNegative(deltaFN);
      if (deltaFN != 0.0) {
        synchronized (this) {
          this.pr = null;
          this.fn += deltaFN;
        }
      }
      return this;
    }

    public Evaluator addTN(final double deltaTN) {
      checkNotNegative(deltaTN);
      if (deltaTN != 0.0) {
        synchronized (this) {
          this.pr = null;
          this.tn += deltaTN;
        }
      }
      return this;
    }

    public synchronized PrecisionRecall getResult() {
      if (this.pr == null) {
        this.pr = forCounts(this.tp, this.fp, this.fn, this.tn);
      }
      return this.pr;
    }

  }

}