package eu.fbk.fm.profiling.extractors;

import com.google.gson.JsonObject;

import java.util.Collection;

public interface Extractor<T, Tfin> {
    void extract(JsonObject tweet, Features features);

    Features.TempFeatureSet<T> merge(Features.TempFeatureSet<T> f1, Features.TempFeatureSet<T> f2);

    T cloneFeatures(T feature);

    Features.FeatureSet<Tfin> fin(Features.TempFeatureSet<T> f1);

    String getId();

    default String statsString() {
        return "Not stats available for extractor "+getId();
    }
}
