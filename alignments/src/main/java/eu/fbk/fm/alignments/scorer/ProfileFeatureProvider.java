package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.text.CosineScorer;
import eu.fbk.fm.alignments.scorer.text.LSAVectorProvider;
import eu.fbk.fm.alignments.scorer.text.SimilarityScorer;
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
        LinkedList<Double> features = new LinkedList<Double>() {{
            add((double) user.getFollowersCount());
            add((double) user.getFriendsCount());
            add((double) user.getListedCount());
            add((double) user.getFavouritesCount());
            add((double) user.getStatusesCount());
            add(user.isProtected() ? 1.0 : 0.0);
            add(user.isVerified() ? 1.0 : 0.0);
            add(user.isGeoEnabled() ? 1.0 : 0.0);
            add(user.isProfileBackgroundTiled() ? 1.0 : 0.0);
            add(user.isProfileUseBackgroundImage() ? 1.0 : 0.0);
            add(user.isDefaultProfile() ? 1.0 : 0.0);
            add(user.isDefaultProfileImage() ? 1.0 : 0.0);
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

    @Override
    public String getSubspaceId() {
        return "profile";
    }
}
