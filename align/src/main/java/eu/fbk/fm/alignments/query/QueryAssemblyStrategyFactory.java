package eu.fbk.fm.alignments.query;

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

    public static QueryAssemblyStrategy get(String strategy) {
        return get(Strategy.valueOf(strategy.toUpperCase()));
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
