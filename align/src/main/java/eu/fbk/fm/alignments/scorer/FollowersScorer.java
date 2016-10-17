package eu.fbk.fm.alignments.scorer;

import twitter4j.User;

/**
 * Score candidates based on the amount of followers
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class FollowersScorer extends AbstractUserScorer {
    @Override
    public double score(User user) {
        if (user.getFollowersCount() <= 0) {
            return 0;
        }

        return Math.log(user.getFollowersCount());
    }
}
