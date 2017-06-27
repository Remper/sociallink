package eu.fbk.fm.alignments.output;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

import java.io.*;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Writes JSON with the results to disk
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class JSONResultWriter implements ResultWriter {
    private final JsonWriter jsonWriter;

    public JSONResultWriter(File output) throws IOException {
        jsonWriter = getGson().newJsonWriter(
                new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(output)), Charsets.UTF_8));
        jsonWriter.beginArray();
    }

    protected Gson getGson() {
        return new Gson();
    }

    @Override
    public void write(String resourceId, List<DumpResource.Candidate> candidates, Long trueUid) {
        try {
            jsonWriter.beginObject()
                    .name("resource_id").value(resourceId)
                    .name("uid").value(trueUid)
                    .name("candidates")
                    .beginArray();
            for (DumpResource.Candidate candidate : candidates) {
                jsonWriter.beginObject()
                        .name("uid").value(candidate.uid)
                        .name("score").value(candidate.score)
                        .endObject();
            }
            jsonWriter.endArray()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {
        try {
            jsonWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        jsonWriter.endArray();
        jsonWriter.close();
    }
}