package eu.fbk.fm.vectorize.preprocessing.text;

import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Same thing as TextExtractor but also returns user ID
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TextExtractorWithId extends TextProcessor implements FlatMapFunction<JsonObject, Tuple3<Long, Long, String>>, MapFunction<JsonObject, Tuple3<Long, Long, String>> {

    private static final long serialVersionUID = 1L;

    public TextExtractorWithId(boolean noCase) {
        super(noCase);
    }

    @Override
    public void flatMap(JsonObject status, Collector<Tuple3<Long, Long, String>> out) throws Exception {
        out.collect(map(status));

        final JsonObject retweet = get(status, JsonObject.class, "retweeted_status");
        if (retweet != null) {
            out.collect(map(retweet));
        }
    }

    @Override
    public Tuple3<Long, Long, String> map(JsonObject status) throws Exception {
        return process(status);
    }
}