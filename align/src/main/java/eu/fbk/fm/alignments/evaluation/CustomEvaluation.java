package eu.fbk.fm.alignments.evaluation;

import eu.fbk.utils.eval.PrecisionRecall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accumulates precision/recall statistics for the alignments pipeline
 */
public class CustomEvaluation {

    private static final Logger logger = LoggerFactory.getLogger(CustomEvaluation.class);

    private boolean alwaysFNwhenNoAlign = false;
    private PrecisionRecall.Evaluator eval;

    public CustomEvaluation() {
        eval = PrecisionRecall.evaluator();
    }

    public CustomEvaluation joint() {
        return joint(true);
    }

    public CustomEvaluation joint(boolean joint) {
        alwaysFNwhenNoAlign = joint;
        return this;
    }

    public void check(int trueValue, int predicted) {
        if (trueValue == predicted) {
            //Prediction aligns
            if (predicted >= 0) {
                //Prediction points to exact candidate
                eval.addTP();
            } else if (alwaysFNwhenNoAlign) {
                //Candidate not in the list (mistake in case of overall evaluation)
                eval.addFN();
            }
        } else {
            //Prediction misaligns: mistake, there is a right candidate somewhere
            if (predicted >= 0) {
                //Wrong prediction (not abstain). Counts as two errors
                eval.addFP();
                if (alwaysFNwhenNoAlign || trueValue != -1) {
                    eval.addFN();
                }
            } else {
                //Abstain. Always counts as false negative
                eval.addFN();
            }
        }
    }

    public PrecisionRecall.Evaluator getEval() {
        return eval;
    }

    public void printResult() {
        PrecisionRecall result = eval.getResult();
        logger.info(String.format("Precision: %6.2f%%", result.getPrecision()*100));
        logger.info(String.format("Recall:    %6.2f%%", result.getRecall()*100));
        logger.info(String.format("F1:        %6.2f%%", result.getF1()*100));
    }

    public String printOneliner() {
        PrecisionRecall result = eval.getResult();
        return String.format("%.4f\t%.4f\t%.4f", result.getPrecision(), result.getRecall(), result.getF1());
    }
}
