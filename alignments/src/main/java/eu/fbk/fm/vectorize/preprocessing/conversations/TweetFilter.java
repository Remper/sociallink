package eu.fbk.fm.vectorize.preprocessing.conversations;

import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import java.util.NavigableSet;

/**
 * Filters tweets based on ID
 */
public class TweetFilter extends RichFilterFunction<Tuple2<Long, JsonObject>> {

    private transient Counter counter;
    private NavigableSet<Long> tweetIds;

    public TweetFilter(NavigableSet<Long> tweetIds) {
        this.tweetIds = tweetIds;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.counter = getRuntimeContext()
            .getMetricGroup()
            .counter("ids_found");
    }

    @Override
    public boolean filter(Tuple2<Long, JsonObject> tweet) throws Exception {
        if (tweetIds.contains(tweet.f0)) {
            this.counter.inc();
            return true;
        }
        return false;
    }
}
