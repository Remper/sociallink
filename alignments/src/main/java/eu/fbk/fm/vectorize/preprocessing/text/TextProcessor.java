package eu.fbk.fm.vectorize.preprocessing.text;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.function.Function;

/**
 * Extracts text from tweet object
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public abstract class TextProcessor implements JsonObjectProcessor, Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean noCase;

    public TextProcessor(boolean noCase) {
        this.noCase = noCase;
    }

    protected Tuple2<Long, String> process(JsonObject status) {
        // Getting user id
        Long id = get(status, Long.class, "user", "id");

        // Get the original text
        String originalText = get(status, String.class, "text");
        if (originalText == null) {
            originalText = get(status, String.class, "full_text");
        }

        // Adding all the replacements
        final LinkedList<Replacement> replacements = new LinkedList<>();

        JsonObject entities = status.getAsJsonObject("entities");
        if (entities == null) {
            return new Tuple2<>(id, originalText);
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

        return new Tuple2<>(id, processedText.trim());
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

    private static class Replacement {
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
