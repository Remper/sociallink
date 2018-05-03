package eu.fbk.fm.vectorize.preprocessing;

import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Extract retweet as a separate entity from tweet if needed
 */
public class ExpandTweet implements FlatMapFunction<JsonObject, JsonObject>, JsonObjectProcessor {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(JsonObject status, Collector<JsonObject> out) {
        out.collect(status);

        //Check if the author of the original tweet is in the list
        final JsonObject retweet = get(status, JsonObject.class, "retweeted_status");
        if (retweet != null ) {
            out.collect(retweet);
        }
    }
}
