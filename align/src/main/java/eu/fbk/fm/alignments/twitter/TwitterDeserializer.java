package eu.fbk.fm.alignments.twitter;

import com.google.gson.*;
import twitter4j.*;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializer for Twitter4j interfaces
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TwitterDeserializer implements JsonDeserializer, JsonSerializer {
    private Map<Type, String> classes = new HashMap<>();

    public void addClass(Type type, String className) {
        classes.put(type, className);
    }

    @Override
    public Object deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        try {
            return jsonDeserializationContext.deserialize(jsonElement, Class.forName(classes.get(type)));
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }
    }

    @Override
    public JsonElement serialize(Object o, Type type, JsonSerializationContext jsonSerializationContext) {
        try {
            return jsonSerializationContext.serialize(o, Class.forName(classes.get(type)));
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }
    }

    public static TwitterDeserializer getDefault() {
        TwitterDeserializer deserializer = new TwitterDeserializer();
        deserializer.addClass(User.class, "twitter4j.UserJSONImpl");
        deserializer.addClass(URLEntity.class, "twitter4j.URLEntityJSONImpl");
        deserializer.addClass(MediaEntity.class, "twitter4j.MediaEntityJSONImpl");
        deserializer.addClass(ExtendedMediaEntity.class, "twitter4j.ExtendedMediaEntityJSONImpl");
        deserializer.addClass(MediaEntity.Size.class, "twitter4j.MediaEntityJSONImpl$Size");
        deserializer.addClass(UserMentionEntity.class, "twitter4j.UserMentionEntityJSONImpl");
        deserializer.addClass(HashtagEntity.class, "twitter4j.HashtagEntityJSONImpl");
        deserializer.addClass(SymbolEntity.class, "twitter4j.HashtagEntityJSONImpl");
        deserializer.addClass(Place.class, "twitter4j.PlaceJSONImpl");
        deserializer.addClass(Status.class, "twitter4j.StatusJSONImpl");
        deserializer.addClass(Scopes.class, "twitter4j.ScopesImpl");
        return deserializer;
    }

    public GsonBuilder getBuilder() {
        GsonBuilder builder = new GsonBuilder();
        for (Map.Entry<Type, String> entry : classes.entrySet()) {
            builder.registerTypeAdapter(entry.getKey(), this);
        }
        return builder;
    }
}