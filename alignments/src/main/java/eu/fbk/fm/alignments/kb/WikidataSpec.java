package eu.fbk.fm.alignments.kb;

import java.util.*;

import static eu.fbk.fm.alignments.kb.ResourceSpec.getProperty;

/**
 * Wikidata resource specification
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class WikidataSpec extends DBpediaSpec {
    public static final String ATTRIBUTE_SKOS_ALT_LABEL = SKOS_ONTOLOGY+"altLabel";
    public static final String ATTRIBUTE_SKOS_PREF_LABEL = SKOS_ONTOLOGY+"prefLabel";
    public static final String ATTRIBUTE_SCHEMA_NAME = SCHEMA_ORG_ONTOLOGY+"name";
    public static final String ATTRIBUTE_SCHEMA_DESCRIPTION = SCHEMA_ORG_ONTOLOGY+"description";

    @Override
    public List<String> extractNames(Map<String, List<String>> attributes) {
        List<String> names = super.extractNames(attributes);
        names.addAll(getProperty(attributes,ATTRIBUTE_SCHEMA_NAME));

        return names;
    }

    @Override
    public List<String> extractLabels(Map<String, List<String>> attributes) {
        List<String> names = super.extractLabels(attributes);
        names.addAll(getProperty(attributes,ATTRIBUTE_SKOS_ALT_LABEL));
        names.addAll(getProperty(attributes,ATTRIBUTE_SKOS_PREF_LABEL));

        return names;
    }

    @Override
    public List<String> extractDescriptions(Map<String, List<String>> attributes) {
        // We need to deduplicate strings before we output
        Set<String> texts = new HashSet<>(super.extractDescriptions(attributes));
        texts.addAll(getProperty(attributes, ATTRIBUTE_SCHEMA_DESCRIPTION));

        return new LinkedList<>(texts);
    }

    @Override
    public List<String> extractGivenNames(Map<String, List<String>> attributes) {
        return super.extractGivenNames(attributes);
    }

    @Override
    public List<String> extractSurnames(Map<String, List<String>> attributes) {
        return super.extractSurnames(attributes);
    }

    @Override
    public EntityType extractType(Map<String, List<String>> attributes) {
        return EntityType.PERSON;
    }

    @Override
    public EntityStatus extractStatus(Map<String, List<String>> attributes) {
        return EntityStatus.ALIVE;
    }
}
