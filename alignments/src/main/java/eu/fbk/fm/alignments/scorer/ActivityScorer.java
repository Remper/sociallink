package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.kb.KBResource;
import twitter4j.User;

import java.util.Date;

/**
 * Scores the date of the post returned with the profile
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ActivityScorer implements FeatureProvider {
    public static final double MAX_DAYS = 100.0;

    private static double daysFromToday(Date date) {
        long difference = new Date().getTime() - date.getTime();
        assert difference > 0;
        return (double) difference / (1000 * 60 * 60 * 24);
    }

    private double unScaledFeature(User user) {
        if (user.getStatus() == null) {
            return MAX_DAYS;
        }
        double days = daysFromToday(user.getStatus().getCreatedAt());
        return days > MAX_DAYS ? MAX_DAYS : days;
    }

    @Override
    public double getFeature(User user, KBResource resource) {
        return unScaledFeature(user);
    }
}
