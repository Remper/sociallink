package eu.fbk.fm.vectorize.preprocessing.conversations;

import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import org.apache.flink.api.common.functions.FilterFunction;

public class LanguageFilter implements FilterFunction<JsonObject>, JsonObjectProcessor {

    private static final long serialVersionUID = 1L;

    private final String[] languages;

    public LanguageFilter(String... languages) {
        this.languages = languages;
    }

    @Override
    public boolean filter(JsonObject value) throws Exception {
        String language = get(value, String.class, "lang");
        for (String compLang : languages) {
            if (language.equals(compLang)) {
                return true;
            }
        }
        return false;
    }
}
