package eu.fbk.ict.fm.ml.features.methods;

/**
 * Accuracy feature selection algorithm
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class MutualInformation implements FeatureSelectionMethod {
  @Override
  public double categoryScore(String term, String category, int A, int B, int C, int D, int N) {
    return A  / ((A + C) * (A + B));
  }
}
