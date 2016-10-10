package eu.fbk.ict.fm.ml.features.methods;

/**
 * Accuracy feature selection algorithm
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Accuracy implements FeatureSelectionMethod {
  @Override
  public double categoryScore(String term, String category, int A, int B, int C, int D, int N) {
    return (double) A / N;
  }
}
