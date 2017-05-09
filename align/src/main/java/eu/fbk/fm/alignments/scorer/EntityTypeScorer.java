package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import twitter4j.User;

import java.util.LinkedList;
import java.util.List;

/**
 * Set of binary features for entity type
 */
public class EntityTypeScorer implements FeatureProvider {
    public enum Type {
        COMPANY, PERSON, OTHER
    }

    private Type type;

    public EntityTypeScorer(Type type) {
        this.type = type;
    }

    @Override
    public double getFeature(User user, DBpediaResource resource) {
        switch (type) {
            case COMPANY:
                return resource.isCompany() ? 1.0d : 0.0d;
            case PERSON:
                return resource.isPerson() ? 1.0d : 0.0d;
            case OTHER:
                return !resource.isPerson() && !resource.isCompany() ? 1.0d : 0.0d;
            default:
                throw new IllegalArgumentException("Unknown entity type");
        }
    }

    public static List<FeatureProvider> createProviders() {
        return new LinkedList<FeatureProvider>(){{
            add(new EntityTypeScorer(Type.PERSON));
            add(new EntityTypeScorer(Type.COMPANY));
            add(new EntityTypeScorer(Type.OTHER));
        }};
    }
}
