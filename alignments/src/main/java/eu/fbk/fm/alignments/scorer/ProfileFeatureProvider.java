package eu.fbk.fm.alignments.scorer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.text.CosineScorer;
import eu.fbk.fm.alignments.scorer.text.LSAVectorProvider;
import eu.fbk.fm.alignments.scorer.text.SimilarityScorer;
import eu.fbk.fm.alignments.twitter.TwitterDeserializer;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.utils.core.strings.JaroWinklerDistance;
import eu.fbk.utils.lsa.LSM;
import twitter4j.User;

import java.util.LinkedList;
import java.util.List;

public class ProfileFeatureProvider implements FeatureVectorProvider, JsonObjectProcessor {

    final SimilarityScorer scorer;
    final List<FeatureVectorProvider> featureProviders;

    public ProfileFeatureProvider(LSM lsa) {
        this.scorer = new CosineScorer(new LSAVectorProvider(lsa));
        this.featureProviders = new LinkedList<FeatureVectorProvider>() {{
            add(new NameScorer(new JaroWinklerDistance()));
            add(new NameScorer.ScreenNameScorer(new JaroWinklerDistance()));
            add(new TextScorer(scorer));
        }};
    }

    @Override
    public double[] getFeatures(User user, DBpediaResource resource) {
        Gson gson = TwitterDeserializer.getDefault().getBuilder().create();
        if (user instanceof UserData) {
            user = ((UserData) user).profile;
        }

        JsonObject userObject = gson.toJsonTree(user).getAsJsonObject();

        LinkedList<Double> features = new LinkedList<Double>() {{
            add(getIntFeature(userObject, "followers_count"));
            add(getIntFeature(userObject, "friends_count"));
            add(getIntFeature(userObject, "listed_count"));
            add(getIntFeature(userObject, "favourites_count"));
            add(getIntFeature(userObject, "statuses_count"));
            add(getBoolFeature(userObject, "protected"));
            add(getBoolFeature(userObject, "verified"));
            add(getBoolFeature(userObject, "geo_enabled"));
            add(getBoolFeature(userObject, "profile_background_tile"));
            add(getBoolFeature(userObject, "profile_use_background_image"));
            add(getBoolFeature(userObject, "default_profile"));
            add(getBoolFeature(userObject, "default_profile_image"));
        }};
        for (FeatureVectorProvider provider : featureProviders) {
            for (double feature : provider.getFeatures(user, resource)) {
                features.add(feature);
            }
        }
        double[] result = new double[features.size()];
        int index = 0;
        for (Double feature : features) {
            result[index] = feature;
            index++;
        }
        return result;
    }

    private double getIntFeature(JsonObject user, String property) {
        Integer value = get(user, Integer.class, property);
        return value == null ? 0.0 : (float) value;
    }

    private double getBoolFeature(JsonObject user, String property) {
        Boolean value = get(user, Boolean.class, property);
        return value == null || !value ? 0.0 : 1.0;
    }

    @Override
    public String getSubspaceId() {
        return "profile";
    }
}
