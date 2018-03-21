package eu.fbk.fm.ml.features.methods;

/**
 * Giuliano feature selection algorithm
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Giuliano implements FeatureSelectionMethod {
  @Override
  public double categoryScore(String term, String category, int A, int B, int C, int D, int N) {
    int F = B + C;
    int min = Math.min(A, F);
    if (min == 0) {
      min = 1;
    }
    return (double) (A - F) / min;
  }
}
