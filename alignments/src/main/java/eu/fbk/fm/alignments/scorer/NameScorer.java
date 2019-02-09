package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.kb.KBResource;
import eu.fbk.utils.core.strings.EditDistance;
import eu.fbk.utils.core.strings.LevenshteinDistance;
import twitter4j.User;

import java.util.List;

/**
 * Score candidates based on the name in their profile
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class NameScorer implements FeatureProvider {
    EditDistance metric;

    public NameScorer() {
        this(new LevenshteinDistance());
    }

    public NameScorer(EditDistance metric) {
        this.metric = metric;
    }

    public boolean isCloseEnough(CharSequence left, CharSequence right) {
        Object result = this.metric.apply(left, right);
        if (result instanceof Integer) {
            return (int) result < 8;
        }
        if (result instanceof Double) {
            return (double) result >= 0.8d;
        }
        throw new IllegalArgumentException("Metric returns a result of an illegal type");
    }

    @Override
    public double getFeature(User user, KBResource resource) {
        return getFeatureForString(user.getName(), resource);
    }

    protected double getFeatureForString(String compareString, KBResource resource) {
        List<String> names = resource.getNames();
        if (names.size() == 0) {
            names.add(resource.getCleanResourceId());
        }
        double average = 0;
        for (String name : names) {
            double result = getDoubleValue(this.metric.apply(compareString.trim().toLowerCase(), name.trim().toLowerCase()));
            average += result;
        }
        return average / names.size();
    }

    private double getDoubleValue(Object result) {
        if (result instanceof Number) {
            return ((Number) result).doubleValue();
        }
        throw new IllegalArgumentException("Metric returns a result of an illegal type");
    }

    private boolean isGreater(double minDistance, Object result) {
        if (result instanceof Integer) {
            return minDistance > ((Integer) result).doubleValue();
        }
        if (result instanceof Double) {
            return minDistance > (Double) result;
        }
        return false;
    }

    public static class ScreenNameScorer extends NameScorer {
        public ScreenNameScorer() {
            this(new LevenshteinDistance());
        }

        public ScreenNameScorer(EditDistance metric) {
            this.metric = metric;
        }

        @Override
        public double getFeature(User user, KBResource resource) {
            return getFeatureForString(user.getScreenName(), resource);
        }
    }
}