package eu.fbk.ict.fm.ml.features.methods;

/**
 * A feature selection method
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface FeatureSelectionMethod {
  /**
   * This method must implement the feature selection metric.
   *
   * @param term     the term
   * @param category the category
   * @param A        number of times the term and category co-occur
   * @param B        number of times the term co-occur without the category
   * @param C        number of times the category co-occur without the term
   * @param D        number of times neither the term nor the category occurs
   * @param N        total number of documents
   */
  double categoryScore(String term, String category, int A, int B, int C, int D, int N);
}
