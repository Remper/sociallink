package eu.fbk.fm.alignments.scorer;

import twitter4j.User;

/**
 * Score candidates higher if they have a 'verified' flag
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class VerifiedScorer extends AbstractUserScorer {
    @Override
    public double score(User user) {
        return user.isVerified() ? 1.0 : 0.0;
    }
}