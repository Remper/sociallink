package eu.fbk.fm.alignments.kb;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static eu.fbk.fm.alignments.kb.ResourceSpec.EntityStatus.DEAD;
import static eu.fbk.fm.alignments.kb.ResourceSpec.EntityType.*;

/**
 * A knowledge base resource that needs to be aligned to the Twitter profile
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public final class KBResource {

    private final String identifier;

    private final List<String> descriptions;
    private final List<String> labels;
    private final List<String> names;
    private final List<String> givenNames;
    private final List<String> surnames;

    private final ResourceSpec.EntityType type;
    private final ResourceSpec.EntityStatus status;

    private final String spec;

    public KBResource(String identifier, ResourceSpec spec, Map<String, List<String>> attributes) {
        this.identifier = identifier;
        this.descriptions = spec.extractDescriptions(attributes);
        this.labels = spec.extractLabels(attributes);
        this.names = spec.extractNames(attributes);
        this.givenNames = spec.extractGivenNames(attributes);
        this.surnames = spec.extractSurnames(attributes);
        this.type = spec.extractType(attributes);
        this.status = spec.extractStatus(attributes);
        this.spec = spec.getClass().getName();
    }

    public String getIdentifier() {
        return identifier;
    }

    public List<String> getNames() {
        return wrap(names);
    }

    public List<String> getLabels() {
        return wrap(labels);
    }

    public List<String> getGivenNames() {
        return wrap(givenNames);
    }

    public List<String> getSurnames() {
        return wrap(surnames);
    }

    public List<String> getDescriptions() {
        return wrap(descriptions);
    }

    public String getSpec() {
        return spec;
    }

    public boolean isPerson() {
        return type == PERSON;
    }

    public boolean isCompany() {
        return type == COMPANY;
    }

    public boolean isOther() {
        return type == OTHER;
    }

    public boolean isDead() {
        return status == DEAD;
    }

    public ResourceSpec.EntityType getType() {
        return type;
    }

    public String getCleanResourceId() {
        String filtered = getIdFromResourceId();
        if (filtered.matches(".+_\\([A-Za-z_]+\\)$")) {
            filtered = filtered.substring(0, filtered.lastIndexOf("_("));
        }
        return filtered.replace('_', ' ');
    }

    public String getTopicFromResourceId() {
        String filtered = getIdFromResourceId();
        if (!filtered.matches(".+_\\([A-Za-z]+\\)$")) {
            return "";
        }

        filtered = filtered.substring(filtered.lastIndexOf("_(") + 2, filtered.length() - 1);
        return filtered.replace('_', ' ');
    }

    private String getIdFromResourceId() {
        String searchString = "resource/";
        int location = identifier.lastIndexOf(searchString);
        if (location == -1) {
            return "";
        }
        return identifier.substring(location + searchString.length());
    }

    private List<String> wrap(List<String> input) {
        return new LinkedList<>(input);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KBResource)) {
            return super.equals(obj);
        }
        return getIdentifier().equals(((KBResource) obj).getIdentifier());
    }

    @Override
    public int hashCode() {
        return getIdentifier().hashCode();
    }
}
