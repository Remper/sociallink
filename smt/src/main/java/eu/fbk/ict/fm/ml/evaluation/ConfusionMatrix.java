package eu.fbk.ict.fm.ml.evaluation;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Ordering;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Computes the Confusion Matrix
 *
 * @author Alessio and Francesco
 */
public final class ConfusionMatrix implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int numLabels;

  private final double[] counts;

  private transient double countTotal;

  @Nullable
  private transient PrecisionRecall[] labelPRs;

  @Nullable
  private transient PrecisionRecall microPR;

  @Nullable
  private transient PrecisionRecall macroPR;

  public ConfusionMatrix(final double[][] matrix) {
    this.numLabels = matrix.length;
    this.counts = new double[this.numLabels * this.numLabels];
    for (int i = 0; i < this.numLabels; ++i) {
      final double[] row = matrix[i];
      Preconditions.checkArgument(row.length == this.numLabels);
      System.arraycopy(row, 0, this.counts, i * this.numLabels, this.numLabels);
    }
  }

  private void checkLabel(final int label) {
    if (label < 0 || label >= this.numLabels) {
      throw new IllegalArgumentException("Invalid label " + label + " (matrix has "
          + this.numLabels + " labels)");
    }
  }

  public int getNumLabels() {
    return this.numLabels;
  }

  public double getCount(final int labelGold, final int labelPredicted) {
    checkLabel(labelGold);
    checkLabel(labelPredicted);
    return this.counts[labelGold * this.numLabels + labelPredicted];
  }

  public double getCountGold(final int label) {
    double count = 0.0;
    for (int i = label * this.numLabels; i < (label + 1) * this.numLabels; ++i) {
      count += this.counts[i];
    }
    return count;
  }

  public double getCountPredicted(final int label) {
    checkLabel(label);
    double count = 0.0;
    for (int i = 0; i < this.numLabels; ++i) {
      count += this.counts[i * this.numLabels + label];
    }
    return count;
  }

  public double getCountTotal() {
    if (this.countTotal == 0.0) {
      double count = 0.0;
      for (int i = 0; i < this.counts.length; ++i) {
        count += this.counts[i];
      }
      this.countTotal = count;
    }
    return this.countTotal;
  }

  public synchronized PrecisionRecall getLabelPR(final int label) {
    if (this.labelPRs == null) {
      this.labelPRs = new PrecisionRecall[this.numLabels];
    }
    if (this.labelPRs[label] == null) {
      final double tp = this.counts[label * this.numLabels + label];
      double fp = 0.0;
      double fn = 0.0;
      for (int i = 0; i < this.numLabels; ++i) {
        if (i != label) {
          fp += this.counts[i * this.numLabels + label];
          fn += this.counts[label * this.numLabels + i];
        }
      }
      final double tn = getCountTotal() - tp - fp - fn;
      this.labelPRs[label] = PrecisionRecall.forCounts(tp, fp, fn, tn);
    }
    return this.labelPRs[label];
  }

  public synchronized PrecisionRecall getMicroPR() {
    if (this.microPR == null) {
      double tp = 0.0;
      for (int i = 0; i < this.numLabels; ++i) {
        tp += this.counts[i * this.numLabels + i];
      }
      final double total = getCountTotal();
      final double fp = total - tp;
      final double fn = fp;
      final double tn = total * this.numLabels - tp - fp - fn;
      this.microPR = PrecisionRecall.forCounts(tp, fp, fn, tn);
    }
    return this.microPR;
  }

  public synchronized PrecisionRecall getMacroPR() {
    if (this.macroPR == null) {
      double p = 0.0;
      double r = 0.0;
      double a = 0.0;
      for (int i = 0; i < this.numLabels; ++i) {
        final PrecisionRecall pr = getLabelPR(i);
        p += pr.getPrecision();
        r += pr.getRecall();
        a += pr.getAccuracy();
      }
      p = p / this.numLabels;
      r = r / this.numLabels;
      a = a / this.numLabels;
      this.macroPR = PrecisionRecall.forMeasures(p, r, a, getCountTotal());
    }
    return this.macroPR;
  }

  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (!(object instanceof ConfusionMatrix)) {
      return false;
    }
    final ConfusionMatrix other = (ConfusionMatrix) object;
    return this.numLabels == other.numLabels && Arrays.equals(this.counts, other.counts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.numLabels, Arrays.hashCode(this.counts));
  }

  @Override
  public String toString() {
    return toString((String[]) null);
  }

  public String toString(@Nullable final String... labelStrings) {

    // Cache some data
    final double total = getCountTotal();
    final PrecisionRecall micro = getMicroPR();
    final PrecisionRecall macro = getMacroPR();

    // Build delimiter
    final String delim = Strings.repeat("-", 8 + 2 + this.numLabels * 10 + 2 + 10 + 10) + '\n';

    // Emit first label row
    final StringBuilder builder = new StringBuilder("pred->   |");
    for (int i = 0; i < this.numLabels; ++i) {
      final String str = labelStrings == null || //
          i >= labelStrings.length ? Integer.toString(i) : labelStrings[i];
      builder.append(String.format("%10s", str));
    }
    builder.append(" |       sum         %\n");
    builder.append(delim);

    // Emit counts + gold sum and percentage
    for (int j = 0; j < this.numLabels; ++j) {
      final double sum = getCountGold(j);
      final String str = labelStrings == null || //
          j >= labelStrings.length ? Integer.toString(j) : labelStrings[j];
      builder.append(String.format("%8s |", str));
      for (int i = 0; i < this.numLabels; ++i) {
        builder.append(String.format("%10.2f", getCount(j, i)));
      }
      builder.append(String.format(" |%10.2f%10.2f\n", sum, sum / total * 100));
    }
    builder.append(delim);

    // Emit predicted sum row
    builder.append("     sum |");
    for (int i = 0; i < this.numLabels; ++i) {
      builder.append(String.format("%10.2f", getCountPredicted(i)));
    }
    builder.append(String.format(" |%10.2f%10.2f\n", total, 100.0));

    // Emit predicted percentage row + micro / macro labels
    builder.append("       % |");
    for (int i = 0; i < this.numLabels; ++i) {
      builder.append(String.format("%10.2f", getCountPredicted(i) / total * 100));
    }
    builder.append(" |     macro     micro\n");
    builder.append(delim);

    // Emit accuracy row
    builder.append("     acc |");
    for (int i = 0; i < this.numLabels; ++i) {
      builder.append(String.format("%10.2f", getLabelPR(i).getAccuracy() * 100));
    }
    builder.append(String.format(" |%10.2f%10.2f\n", macro.getAccuracy() * 100,
        micro.getAccuracy() * 100));

    // Emit precision row
    builder.append("    prec |");
    for (int i = 0; i < this.numLabels; ++i) {
      builder.append(String.format("%10.2f", getLabelPR(i).getPrecision() * 100));
    }
    builder.append(String.format(" |%10.2f%10.2f\n", macro.getPrecision() * 100,
        micro.getPrecision() * 100));

    // Emit recall row
    builder.append("     rec |");
    for (int i = 0; i < this.numLabels; ++i) {
      builder.append(String.format("%10.2f", getLabelPR(i).getRecall() * 100));
    }
    builder.append(String.format(" |%10.2f%10.2f\n", macro.getRecall() * 100,
        micro.getRecall() * 100));

    // Emit F1 row
    builder.append("      F1 |");
    for (int i = 0; i < this.numLabels; ++i) {
      builder.append(String.format("%10.2f", getLabelPR(i).getF1() * 100));
    }
    builder.append(String.format(" |%10.2f%10.2f\n", macro.getF1() * 100, micro.getF1() * 100));

    // Return constructed table string
    return builder.toString();
  }

  public static Ordering<ConfusionMatrix> labelComparator(final PrecisionRecall.Measure measure,
                                                          final int label, final boolean higherIsBetter) {
    return new Ordering<ConfusionMatrix>() {

      @Override
      public int compare(final ConfusionMatrix left, final ConfusionMatrix right) {
        final double leftValue = left.getLabelPR(label).get(measure);
        final double rightValue = right.getLabelPR(label).get(measure);
        if (Double.isNaN(leftValue)) {
          return Double.isNaN(rightValue) ? 0 : 1;
        } else {
          return Double.isNaN(rightValue) ? -1 : Double.compare(leftValue, rightValue)
              * (higherIsBetter ? -1 : 1);
        }
      }

    };
  }

  public static Ordering<ConfusionMatrix> microComparator(final PrecisionRecall.Measure measure,
                                                          final boolean higherIsBetter) {
    return new Ordering<ConfusionMatrix>() {

      @Override
      public int compare(final ConfusionMatrix left, final ConfusionMatrix right) {
        final double leftValue = left.getMicroPR().get(measure);
        final double rightValue = right.getMicroPR().get(measure);
        final int result = Double.compare(leftValue, rightValue);
        return higherIsBetter ? -result : result;
      }

    };
  }

  public static Ordering<ConfusionMatrix> macroComparator(final PrecisionRecall.Measure measure,
                                                          final boolean higherIsBetter) {
    return new Ordering<ConfusionMatrix>() {

      @Override
      public int compare(final ConfusionMatrix left, final ConfusionMatrix right) {
        final double leftValue = left.getMacroPR().get(measure);
        final double rightValue = right.getMacroPR().get(measure);
        final int result = Double.compare(leftValue, rightValue);
        return higherIsBetter ? -result : result;
      }

    };
  }

  @Nullable
  public static ConfusionMatrix sum(final Iterable<ConfusionMatrix> matrixes) {

    // Compute number of matrixes and number of labels
    int numMatrixes = 0;
    int numLabels = 0;
    for (final ConfusionMatrix matrix : matrixes) {
      ++numMatrixes;
      numLabels = Math.max(numLabels, matrix.numLabels);
    }

    // Compute result, differentiating based on number of matrixes
    if (numMatrixes == 0) {
      return null;
    } else if (numMatrixes == 1) {
      return matrixes.iterator().next();
    } else {
      final double[][] counts = new double[numLabels][];
      for (int i = 0; i < numLabels; ++i) {
        counts[i] = new double[numLabels];
        for (int j = 0; j < numLabels; ++j) {
          for (final ConfusionMatrix matrix : matrixes) {
            if (i < matrix.getNumLabels() && j < matrix.getNumLabels()) {
              counts[i][j] += matrix.getCount(i, j);
            }
          }
        }
      }
      return new ConfusionMatrix(counts);
    }
  }

  public static Evaluator evaluator(final int numLabels) {
    return new Evaluator(numLabels);
  }

  public static final class Evaluator {

    private final double[][] counts;

    @Nullable
    private ConfusionMatrix cachedResult;

    private Evaluator(final int numLabels) {
      this.counts = new double[numLabels][];
      for (int i = 0; i < numLabels; ++i) {
        this.counts[i] = new double[numLabels];
      }
      this.cachedResult = null;
    }

    public synchronized Evaluator add(final int labelGold, final int labelPredicted,
                                      final double count) {
      this.cachedResult = null;
      this.counts[labelGold][labelPredicted] += count;
      return this;
    }

    public synchronized Evaluator add(final ConfusionMatrix matrix) {
      this.cachedResult = null;
      final int numLabels = Math.min(this.counts.length, matrix.getNumLabels());
      for (int i = 0; i < numLabels; ++i) {
        for (int j = 0; j < numLabels; ++j) {
          this.counts[i][j] += matrix.getCount(i, j);
        }
      }
      return this;
    }

    public synchronized Evaluator add(final ConfusionMatrix.Evaluator evaluator) {
      this.cachedResult = null;
      final int numLabels = Math.min(this.counts.length, evaluator.counts.length);
      synchronized (evaluator) {
        for (int i = 0; i < numLabels; ++i) {
          for (int j = 0; j < numLabels; ++j) {
            this.counts[i][j] += evaluator.counts[i][j];
          }
        }
      }
      return this;
    }

    public synchronized ConfusionMatrix getResult() {
      if (this.cachedResult == null) {
        this.cachedResult = new ConfusionMatrix(this.counts);
      }
      return this.cachedResult;
    }

  }

}
