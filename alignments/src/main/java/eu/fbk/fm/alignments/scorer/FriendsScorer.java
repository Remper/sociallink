package eu.fbk.fm.alignments.scorer;

import twitter4j.User;

/**
 * Score candidates based on the amount of friends
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class FriendsScorer extends AbstractUserScorer {
    @Override
    public double score(User user) {
        if (user.getFriendsCount() <= 0) {
            return 0;
        }

        return Math.log(user.getFriendsCount());
    }
}
