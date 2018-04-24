package eu.fbk.fm.vectorize.preprocessing.conversations;

import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class ConversationExtractor implements FlatMapFunction<JsonObject, Tuple3<Long, Long, JsonObject>>, JsonObjectProcessor {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(JsonObject status, Collector<Tuple3<Long, Long, JsonObject>> out) throws Exception {
        final Long sourceId = get(status, Long.class, "in_reply_to_status_id");
        if (sourceId != null) {
            final Long targetId = get(status, Long.class, "id");
            out.collect(new Tuple3<>(sourceId, targetId, status));
        }

        //Check if the author of the original tweet is in the list
        final JsonObject retweet = get(status, JsonObject.class, "retweeted_status");
        if (retweet != null ) {
            flatMap(retweet, out);
        }
    }
}
