package eu.fbk.ict.fm.ml.features.methods;

/**
 * ChiSquared feature selection algorithm
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ChiSquared implements FeatureSelectionMethod {
  @Override
  public double categoryScore(String term, String category, int A, int B, int C, int D, int N) {
    return 0;
  }
}
