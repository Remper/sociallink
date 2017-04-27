package eu.fbk.fm.alignments.query;

import javax.annotation.Nullable;

/**
 * A factory for the query assembly strategies
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class QueryAssemblyStrategyFactory {
    public enum Strategy {
        NO_QOUTES, STRICT_QOUTES, STRICT_TOPIC, STRICT, DEFAULT
    }

    public static QueryAssemblyStrategy def() {
        return get(Strategy.DEFAULT);
    }

    public static QueryAssemblyStrategy get(@Nullable String strategyString) {
        Strategy strategy = Strategy.DEFAULT;
        if (strategyString != null) {
            strategy = Strategy.valueOf(strategyString.toUpperCase());
        }
        return get(strategy);
    }

    public static QueryAssemblyStrategy get(Strategy strategy) {
        switch (strategy) {
            default:
            case DEFAULT:
            case STRICT:
                return new StrictStrategy();
            case STRICT_QOUTES:
                return new StrictQuotesStrategy();
            case STRICT_TOPIC:
                return new StrictWithTopicStrategy();
            case NO_QOUTES:
                return new NoQuotesDupesStrategy();
        }
    }
}
