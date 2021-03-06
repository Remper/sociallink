package eu.fbk.fm.vectorize.preprocessing.text;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
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

    protected Tuple3<Long, Long, String> process(JsonObject status) {
        // Getting user id
        Long userId = get(status, Long.class, "user", "id");

        // Getting tweet id
        Long id = get(status, Long.class, "id");

        // Get the original text
        String originalText = get(status, String.class, "text");
        if (originalText == null) {
            originalText = get(status, String.class, "full_text");
        }

        // Adding all the replacements
        final LinkedList<Replacement> replacements = new LinkedList<>();

        JsonObject entities = status.getAsJsonObject("entities");
        if (entities != null) {
            replacements.addAll(addReplacements(entities, "hashtags", hashtag -> breakHashtag(hashtag.get("text").getAsString())));
            replacements.addAll(addReplacements(entities, "user_mentions", mention -> mention.get("name").getAsString()));
            replacements.addAll(addReplacements(entities, "urls", url -> " <url> "));
            replacements.addAll(addReplacements(entities, "media", media -> " <media> "));
        }

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

        //Clean and separate emojis
        String processedText = cleanAndSeparateEmoji(sb.toString());

        //Getting rid of RT pattern, excess whitespace and sneaky urls
        processedText = processedText
            .replaceAll("^RT ", "")
            //.replaceAll("^\\.", "")
            //.replaceAll("\\.$", "")
            .replaceAll("https?://[^\\s]+", " <url> ")
            //.replaceAll("[,.?_!@#$%^&*():|/\\\\]", " ")
            //.replaceAll("[\"'`‘“´]", " ' ")
            .replaceAll("\\s+", " ");
        if (noCase) {
            processedText = processedText.toLowerCase();
        }

        return new Tuple3<>(id, userId, prepareString(processedText.trim()));
    }

    private String cleanAndSeparateEmoji(String source) {
        StringBuilder buffer = new StringBuilder();
        AtomicBoolean prevEmoji = new AtomicBoolean(false);

        source.codePoints().forEach(value -> {
            if ((value >= 0x1F3FB && value <= 0x1F3FF) // Fitzpatrick diversity modifiers (skip)
                    || value == 0x200D) { // Glue character (skip)
                return;
            }

            if ((value >= 0x1F600 && value <= 0x1F64F) // Emoticons
                    || (value >= 0x1F900 && value <= 0x1F9FF) // Supplemental Symbols and Pictograms
                    || (value >= 0x2600 && value <= 0x26FF) // Miscellaneous Symbols
                    || (value >= 0x2700 && value <= 0x27BF) // Dingbats
                    || (value >= 0x1F300 && value <= 0x1F5FF) // Miscellaneous Symbols And Pictographs (Emoji)
                    || (value >= 0x1F1E6 && value <= 0x1F1FF)) // Flags
            {
                if (!prevEmoji.get()) {
                    buffer.append(' ');
                }
                prevEmoji.set(true);
            } else {
                prevEmoji.set(false);
            }

            if (prevEmoji.get()) {
                buffer.append(' ');
            }
            buffer.appendCodePoint(value);
        });

        return buffer.toString();
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

    private static String prepareString(String source) {
        StringBuilder buffer = new StringBuilder();

        source.codePoints().forEach(value -> {
            if (value == 0x00) {
                return;
            }
            buffer.appendCodePoint(value);
        });

        return buffer.toString();
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
