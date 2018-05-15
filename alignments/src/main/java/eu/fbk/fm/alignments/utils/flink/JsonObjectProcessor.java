package eu.fbk.fm.alignments.utils.flink;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

/**
 * Trait that provides a helper function for shorthand processing of JsonObjects
 */
public interface JsonObjectProcessor {
    @SuppressWarnings("unchecked")
    default <T> T get(final JsonElement json, final Class<T> clazz, final String... path) {
        JsonElement result = json;
        for (final String element : path) {
            result = result instanceof JsonObject ? ((JsonObject) result).get(element) : null;
        }
        if (result == null || result instanceof JsonNull) {
            return null;
        } else if (clazz.isInstance(result)) {
            return clazz.cast(result);
        } else if (clazz == Long.class) {
            return (T) (Long) result.getAsLong();
        } else if (clazz == Float.class) {
            return (T) (Float) result.getAsFloat();
        } else if (clazz == String.class) {
            return (T) result.getAsString();
        } else if (clazz == Integer.class) {
            return (T) (Integer) result.getAsInt();
        } else if (clazz == Boolean.class) {
            return (T) (Boolean) result.getAsBoolean();
        } else {
            throw new UnsupportedOperationException(clazz.getName());
        }
    }
}
