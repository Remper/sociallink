package eu.fbk.fm.ml.features.methods;

/**
 * Aprosio feature selection algorithm
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Aprosio implements FeatureSelectionMethod {
  @Override
  public double categoryScore(String term, String category, int A, int B, int C, int D, int N) {
    int min = Math.min(A, B);
    if (min == 0) {
      min = 1;
    }
    return (double) (A - B) / min;
  }
}
