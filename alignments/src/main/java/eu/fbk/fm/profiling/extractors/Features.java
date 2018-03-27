package eu.fbk.fm.profiling.extractors;

import java.util.*;
import java.util.stream.Collectors;

public class Features {
    private final Map<String, TempFeatureSet> features;

    public Features() {
        this.features = new HashMap<>();
    }

    public Collection<FeatureSet> getFeatures() {
        return features.values().parallelStream()
                .map(TempFeatureSet::fin)
                .collect(Collectors.toList());
    }

    public <T> void addFeatureSet(TempFeatureSet.Type type, String name, T features, Extractor<T, ?> producer) {
        this.addFeatureSet(type, name, features, 0L, producer);
    }

    public synchronized <T> void addFeatureSet(TempFeatureSet.Type type, String name, T features, Long timestamp, Extractor<T, ?> producer) {
        TempFeatureSet<T> newFeature = new TempFeatureSet<>(type, name, features, timestamp, producer);
        TempFeatureSet<T> oldFeature = this.features.get(name);
        this.features.put(name, newFeature.merge(oldFeature));
    }

    public static class FeatureSet<T> {

        public final String name;
        public final String id;
        public final T features;
        public final Long timestamp;

        public FeatureSet(String name, String id, T features, Long timestamp) {
            this.name = name;
            this.id = id;
            this.features = features;
            this.timestamp = timestamp;
        }
    }

    public static class TempFeatureSet<T> {
        public enum Type{MAX, AVG}

        private final String name;
        private final T features;
        private final Long timestamp;
        private final Type type;
        private final Extractor<T, ?> producer;

        public TempFeatureSet(Type type, String name, T features, Long timestamp, Extractor<T, ?> producer) {
            this.type = type;
            this.name = name;
            this.features = features;
            this.timestamp = timestamp;
            this.producer = producer;
        }

        @Override
        public TempFeatureSet<T> clone() {
            return new TempFeatureSet<>(type, name, producer.cloneFeatures(features), timestamp, producer);
        }

        public TempFeatureSet<T> merge(TempFeatureSet<T> set) {
            if (set == null) {
                return this;
            }

            if (set.getType() == Type.MAX) {
                if (set.getTimestamp() > timestamp) {
                    return set.clone();
                } else {
                    return this.clone();
                }
            }

            return producer.merge(this, set);
        }

        public String getName() {
            return name;
        }

        public T getFeatures() {
            return features;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public Type getType() {
            return type;
        }

        public Extractor<T, ?> getProducer() {
            return producer;
        }

        public FeatureSet fin() {
            return producer.fin(this);
        }
    }
}