package eu.fbk.fm.alignments.index.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class Deserializer implements FlatMapFunction<String, JsonObject>, JsonObjectProcessor {

    private static final long serialVersionUID = 1L;

    private static final Gson GSON = new Gson();

    @Override
    public void flatMap(String value, Collector<JsonObject> out) {
        try {
            JsonObject object = GSON.fromJson(value, JsonObject.class);

            if (object == null) {
                return;
            }

            final Long source = get(object, Long.class, "user", "id");
            if (source == null) {
                return;
            }

            out.collect(object);
        } catch (final Throwable e) {
            //Don't care much about thrown away records
        }
    }
}
