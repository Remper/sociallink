package eu.fbk.fm.vectorize.preprocessing.text;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.function.Function;

/**
 * A flat map piece of pipeline that extracts and tokenizes text from tweets
 */
public class TextExtractor implements FlatMapFunction<JsonObject, String>, MapFunction<JsonObject, String>, JsonObjectProcessor {

    private static final long serialVersionUID = 1L;

    private final boolean noCase;

    public TextExtractor(boolean noCase) {
        this.noCase = noCase;
    }

    @Override
    public void flatMap(JsonObject status, Collector<String> out) {
        out.collect(map(status));

        //Check if the author of the original tweet is in the list
        final JsonObject retweet = get(status, JsonObject.class, "retweeted_status");
        if (retweet != null ) {
            out.collect(map(retweet));
        }
    }

    @Override
    public String map(JsonObject status) {
        return process(status);
    }

    public String map(JsonElement status) {
        return process(status.getAsJsonObject());
    }

    protected String process(JsonObject status) {
        // Get the original text
        final String originalText = get(status, String.class, "text");

        // Adding all the replacements
        final LinkedList<Replacement> replacements = new LinkedList<>();

        JsonObject entities = status.getAsJsonObject("entities");
        if (entities == null) {
            return originalText;
        }

        replacements.addAll(addReplacements(entities, "hashtags", hashtag -> breakHashtag(hashtag.get("text").getAsString())));
        replacements.addAll(addReplacements(entities, "user_mentions", mention -> " <mention> "));
        replacements.addAll(addReplacements(entities, "urls", url -> " <url> "));
        replacements.addAll(addReplacements(entities, "media", media -> " <media> "));

        // Sorting replacements
        replacements.sort(Comparator.comparingInt(r -> r.start));

        // Replace all the entities in the original text
        final StringBuilder sb = new StringBuilder();
        final int[] i = {-1};
        originalText.codePoints().forEachOrdered(value -> {
            i[0]++;
            Replacement nextReplacement = replacements.peekFirst();
            if (nextReplacement == null || i[0] < nextReplacement.start) {
                sb.appendCodePoint(value);
                return;
            }

            if (i[0] < nextReplacement.finish-1) {
                return;
            }

            sb.append(nextReplacement.replacement);
            replacements.pollFirst();
        });

        String processedText = sb.toString();

        //Getting rid of RT pattern, excess whitespace and sneaky urls
        processedText = processedText
                .replaceAll("^RT ", "")
                .replaceAll("^\\.", "")
                .replaceAll("\\.$", "")
                .replaceAll("https?://[^\\s]+", " <url> ")
                .replaceAll("[,.?!@#$%^&*():|]", " ")
                .replaceAll("[\"'`‘“´]", " ' ")
                .replaceAll("\\s+", " ");
        if (noCase) {
            processedText = processedText.toLowerCase();
        }

        return processedText.trim();
    }

    private String breakHashtag(String hashtag) {
        StringBuilder sb = new StringBuilder();
        sb.append(" <shash> ");
        final boolean[] prevUppercase = {false};
        hashtag.codePoints().forEachOrdered(value -> {
            if (Character.isUpperCase(value)) {
                if(sb.length() > 0 && !prevUppercase[0]) {
                    sb.append(' ');
                }
                prevUppercase[0] = true;
            } else {
                prevUppercase[0] = false;
            }
            sb.appendCodePoint(value);
        });
        sb.append(" <ehash> ");

        return sb.toString();
    }

    private LinkedList<Replacement> addReplacements(JsonObject entities, String entityName, Function<JsonObject, String> replacementFunc) {
        LinkedList<Replacement> replacements = new LinkedList<>();

        JsonArray entityArray = entities.getAsJsonArray(entityName);
        if (entityArray != null) {
            for (JsonElement entity : entityArray) {
                JsonObject entityObj = entity.getAsJsonObject();
                JsonArray indices = entityObj.getAsJsonArray("indices");
                String replacement = replacementFunc.apply(entityObj);
                replacements.add(new Replacement(indices.get(0).getAsInt(), indices.get(1).getAsInt(), replacement));
            }
        }

        return replacements;
    }

    private final class Replacement {
        int start;
        int finish;
        String replacement;

        public Replacement(int start, int finish, String replacement) {
            this.start = start;
            this.finish = finish;
            this.replacement = replacement;
        }
    }
}
