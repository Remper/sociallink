package eu.fbk.fm.vectorize.preprocessing.text;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

/**
 * A flat map piece of pipeline that extracts and tokenizes text from tweets
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TextExtractor extends TextProcessor implements FlatMapFunction<JsonObject, String>, MapFunction<JsonObject, String> {

    private static final long serialVersionUID = 1L;

    public TextExtractor(boolean noCase) {
        super(noCase);
    }

    @Override
    public void flatMap(JsonObject status, Collector<String> out) {
        out.collect(map(status));

        final JsonObject retweet = get(status, JsonObject.class, "retweeted_status");
        if (retweet != null) {
            out.collect(map(retweet));
        }
    }

    @Override
    public String map(JsonObject status) {
        return process(status).f2;
    }

    public String map(JsonElement status) {
        return process(status.getAsJsonObject()).f2;
    }





}
