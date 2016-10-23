package eu.fbk.ict.fm.smt.model;

/**
 * A single instance of candidate-based score
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Score {
    public String username;
    public double score;
    public Object debug = null;

    public Score(String username, double score) {
        this.username = username;
        this.score = score;
    }
}
