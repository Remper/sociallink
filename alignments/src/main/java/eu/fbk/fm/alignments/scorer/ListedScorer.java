package eu.fbk.fm.alignments.scorer;

import twitter4j.User;

/**
 * Score based on the amount of lists to which the user is included
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ListedScorer extends AbstractUserScorer {
    @Override
    public double score(User user) {
        if (user.getListedCount() == 0) {
            return 0;
        }

        return Math.log(user.getListedCount());
    }
}
