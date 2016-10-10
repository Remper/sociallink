package eu.fbk.ict.fm.ml.features.methods;

/**
 * Accuracy feature selection algorithm
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class OddsRatio implements FeatureSelectionMethod {
  @Override
  public double categoryScore(String term, String category, int A, int B, int C, int D, int N) {
    double score = (double) (A * D) / (B * C);

    if (score == Double.POSITIVE_INFINITY) {
      score = Double.MAX_VALUE;
    }
    return score;
  }
}
