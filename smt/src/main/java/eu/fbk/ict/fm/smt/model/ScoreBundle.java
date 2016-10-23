package eu.fbk.ict.fm.smt.model;

import java.util.LinkedList;
import java.util.List;

/**
 * Set of named candidate-based scores
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ScoreBundle {
    public String type;
    public List<Score> scores;

    public ScoreBundle(String type, List<Score> scores) {
        this.type = type;
        this.scores = scores;
    }

    public ScoreBundle(String type) {
        this.type = type;
        this.scores = new LinkedList<>();
    }
}
