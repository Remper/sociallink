package eu.fbk.fm.alignments.scorer;

import twitter4j.User;

/**
 * Ratio between the amount of friends and followers
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class FollowersFriendsRatioScorer extends AbstractUserScorer {
    @Override
    public double score(User user) {
        if (user.getFriendsCount() <= 0 || user.getFollowersCount() <= 0) {
            return 0;
        }

        return Math.log((double) user.getFollowersCount() / user.getFriendsCount());
    }
}
