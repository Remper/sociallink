package eu.fbk.fm.alignments.scorer.text;

import eu.fbk.utils.math.Vector;

/**
 * Provides a feature vector based on the input text
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface VectorProvider {
    Vector toVector(String text);

    interface DebuggableVectorProvider extends VectorProvider, Debuggable { }
}
