package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.kb.KBResource;
import eu.fbk.fm.alignments.kb.ResourceSpec;
import eu.fbk.fm.alignments.kb.ResourceSpec.EntityType;
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

    private EntityType type;

    public EntityTypeScorer(EntityType type) {
        this.type = type;
    }

    @Override
    public double getFeature(User user, KBResource resource) {
        switch (type) {
            case COMPANY:
                return resource.isCompany() ? 1.0d : 0.0d;
            case PERSON:
                return resource.isPerson() ? 1.0d : 0.0d;
            case OTHER:
                return resource.isOther() ? 1.0d : 0.0d;
            default:
                throw new IllegalArgumentException("Unknown entity type");
        }
    }

    public static List<FeatureProvider> createProviders() {
        return new LinkedList<FeatureProvider>(){{
            add(new EntityTypeScorer(EntityType.PERSON));
            add(new EntityTypeScorer(EntityType.COMPANY));
            add(new EntityTypeScorer(EntityType.OTHER));
        }};
    }
}
