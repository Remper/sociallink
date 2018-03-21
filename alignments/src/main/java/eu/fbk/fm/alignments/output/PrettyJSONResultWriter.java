package eu.fbk.fm.alignments.output;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;

/**
 * Writes pretty JSON with the results to disk
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class PrettyJSONResultWriter extends JSONResultWriter {
    public PrettyJSONResultWriter(File output) throws IOException {
        super(output);
    }

    @Override
    protected Gson getGson() {
        return new GsonBuilder().setPrettyPrinting().create();
    }
}