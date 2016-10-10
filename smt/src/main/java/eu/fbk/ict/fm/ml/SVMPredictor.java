package eu.fbk.ict.fm.ml;

import eu.fbk.ict.fm.data.dataset.Binary;
import org.fbk.cit.hlt.core.mylibsvm.svm;
import org.fbk.cit.hlt.core.mylibsvm.svm_model;
import org.fbk.cit.hlt.core.mylibsvm.svm_node;

import java.io.IOException;

import static org.fbk.cit.hlt.core.mylibsvm.svm.svm_predict_values;

/**
 * A simple wrapper for LIBSVM to enable in-memory SVM predictions
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class SVMPredictor {
  protected svm_model model;
  protected String modelFile;
  protected boolean supportProbability;

  public SVMPredictor(Binary model) throws IOException {
    this(model.getSource().getAbsolutePath());
  }

  public SVMPredictor(String filename) throws IOException {
    this.modelFile = filename;
    this.model = svm.svm_load_model(filename);
    this.supportProbability = svm.svm_check_probability_model(this.model) == 1;
  }

  public boolean isSupportProbability() {
    return supportProbability;
  }

  public svm_node[] nodes(double[] x) {
    svm_node[] nodes = new svm_node[x.length];
    for (int i = 0; i < nodes.length; i++) {
      svm_node node = new svm_node();
      node.index = i + 1;
      node.value = x[i];
      nodes[i] = node;
    }
    return nodes;
  }

  public double predict(svm_node[] x) {
    int numberOfClasses = svm.svm_get_nr_class(model);
    double[] probabilityEstimates = new double[numberOfClasses];
    double result;

    //TODO: return probability estimates in both cases
    if (isSupportProbability()) {
      result = svm.svm_predict_probability(model, x, probabilityEstimates);
    } else {
      result = svm.svm_predict(model, x);
    }
    return result;
  }

  public double predictBinaryWithScore(svm_node[] x) {
    double[] dec_values = new double[1];
    svm_predict_values(model, x, dec_values);

    return dec_values[0];
  }

  public String getModelFile() {
    return modelFile;
  }
}
