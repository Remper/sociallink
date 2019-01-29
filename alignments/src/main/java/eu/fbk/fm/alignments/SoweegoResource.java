package eu.fbk.fm.alignments;

import java.util.List;
import java.util.Map;

/**
 * A Wikidata entity that is always a person
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class SoweegoResource extends DBpediaResource {
    public static final String SCHEMA_ORG_ONT = "http://schema.org/";
    public static final String SKOS_ONT = "http://www.w3.org/2004/02/skos/core#";

    public static final String ATTRIBUTE_SKOS_ALT_LABEL = SKOS_ONT+"altLabel";
    public static final String ATTRIBUTE_SKOS_PREF_LABEL = SKOS_ONT+"prefLabel";
    public static final String ATTRIBUTE_SCHEMA_NAME = SCHEMA_ORG_ONT+"name";
    public static final String ATTRIBUTE_SCHEMA_DESCRIPTION = SCHEMA_ORG_ONT+"description";

    public SoweegoResource(String identifier, Map<String, List<String>> attributes) {
        super(identifier, attributes);
    }

    @Override
    public List<String> getNames() {
        List<String> names = super.getNames();
        names.addAll(getProperty(ATTRIBUTE_SCHEMA_NAME));

        return names;
    }

    public List<String> getLabels()  {
        List<String> labels = super.getLabels();
        labels.addAll(getProperty(ATTRIBUTE_SKOS_ALT_LABEL));
        labels.addAll(getProperty(ATTRIBUTE_SKOS_PREF_LABEL));

        return labels;
    }

    public List<String> getDescriptions() {
        List<String> texts = super.getDescriptions();
        texts.addAll(getProperty(ATTRIBUTE_SCHEMA_DESCRIPTION));

        return texts;
    }

    @Override
    public boolean isCompany() {
        return false;
    }

    @Override
    public boolean isPerson() {
        return true;
    }

    @Override
    public boolean isOther() {
        return false;
    }
}
