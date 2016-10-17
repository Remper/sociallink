package eu.fbk.fm.alignments.scorer;

import twitter4j.User;

/**
 * Scores based on the amount of statuses
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class StatusesScorer extends AbstractUserScorer {
    @Override
    public double score(User user) {
        if (user.getStatusesCount() == 0) {
            return 0;
        }

        return Math.log(user.getStatusesCount());
    }
}
