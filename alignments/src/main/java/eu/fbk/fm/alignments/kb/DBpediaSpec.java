package eu.fbk.fm.alignments.kb;

import java.util.List;
import java.util.Map;

import static eu.fbk.fm.alignments.kb.ResourceSpec.getProperty;
import static eu.fbk.fm.alignments.kb.ResourceSpec.hasProperty;

/**
 * DBpedia resource specification
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class DBpediaSpec implements ResourceSpec {

    public static final String ALIGNMENTS_PERSON = ALIGNMENTS_ONTOLOGY + "Person";
    public static final String ALIGNMENTS_ORGANISATION = ALIGNMENTS_ONTOLOGY + "Organisation";

    public static final String ATTRIBUTE_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    public static final String ATTRIBUTE_LABEL = RDFS_ONTOLOGY+"label";
    public static final String ATTRIBUTE_NAME = FOAF_ONTOLOGY+"name";
    public static final String ATTRIBUTE_GIVEN_NAME = FOAF_ONTOLOGY+"givenName";
    public static final String ATTRIBUTE_SURNAME = FOAF_ONTOLOGY+"surname";

    public static final String COMMENT_PROPERTY = RDFS_ONTOLOGY+"comment";
    public static final String ABSTRACT_PROPERTY = DBPEDIA_ONTOLOGY + "abstract";

    public static final String ATTRIBUTE_DEATHDATE = DBPEDIA_ONTOLOGY + "deathDate";
    public static final String ATTRIBUTE_DEATHDATE_WD = WIKIDATA_ENTITY + "P570";
    public static final String ATTRIBUTE_DEATHYEAR = DBPEDIA_ONTOLOGY + "deathYear";
    public static final String ATTRIBUTE_DEATHPLACE = DBPEDIA_ONTOLOGY + "deathPlace";
    public static final String ATTRIBUTE_DEATHPLACE_WD = WIKIDATA_ENTITY + "P20";
    public static final String ATTRIBUTE_DEATHCAUSE = DBPEDIA_ONTOLOGY + "deathCause";
    public static final String ATTRIBUTE_DEATHCAUSE_WD = WIKIDATA_ENTITY + "P509";
    public static final String ATTRIBUTE_DEATHCAUSE_REVERSED = DBPEDIA_ONTOLOGY + "causeOfDeath";
    public static final String[] DEAD_PERSON_ATTRIBUTES = {
            ATTRIBUTE_DEATHDATE, ATTRIBUTE_DEATHDATE_WD, ATTRIBUTE_DEATHPLACE, ATTRIBUTE_DEATHPLACE_WD,
            ATTRIBUTE_DEATHCAUSE, ATTRIBUTE_DEATHCAUSE_WD, ATTRIBUTE_DEATHCAUSE_REVERSED, ATTRIBUTE_DEATHYEAR
    };

    public static final String ATTRIBUTE_EXTINCTIONYEAR = DBPEDIA_ONTOLOGY + "extinctionYear";
    public static final String ATTRIBUTE_EXTINCTIONDATE = DBPEDIA_ONTOLOGY + "extinctionDate";
    public static final String ATTRIBUTE_CLOSINGYEAR = DBPEDIA_ONTOLOGY + "closingYear";
    public static final String ATTRIBUTE_CLOSE = DBPEDIA_PROPERTY + "close";
    public static final String ATTRIBUTE_CLOSED = DBPEDIA_PROPERTY + "closed";
    public static final String ATTRIBUTE_DEFUNCT = DBPEDIA_PROPERTY + "defunct";
    public static final String[] DEAD_COMPANY_ATTRIBUTES = {
            ATTRIBUTE_EXTINCTIONYEAR, ATTRIBUTE_EXTINCTIONDATE, ATTRIBUTE_CLOSINGYEAR,
            ATTRIBUTE_DEFUNCT, ATTRIBUTE_CLOSE, ATTRIBUTE_CLOSED
    };

    @Override
    public List<String> extractNames(Map<String, List<String>> attributes) {
        return getProperty(attributes, ATTRIBUTE_NAME);
    }

    @Override
    public List<String> extractLabels(Map<String, List<String>> attributes) {
        return getProperty(attributes, ATTRIBUTE_LABEL);
    }

    @Override
    public List<String> extractDescriptions(Map<String, List<String>> attributes) {
        List<String> texts = getProperty(attributes, ABSTRACT_PROPERTY);
        texts.addAll(getProperty(attributes, COMMENT_PROPERTY));

        return texts;
    }

    @Override
    public List<String> extractGivenNames(Map<String, List<String>> attributes) {
        return getProperty(attributes, ATTRIBUTE_GIVEN_NAME);
    }

    @Override
    public List<String> extractSurnames(Map<String, List<String>> attributes) {
        return getProperty(attributes, ATTRIBUTE_SURNAME);
    }

    @Override
    public EntityType extractType(Map<String, List<String>> attributes) {
        if (hasProperty(attributes, ATTRIBUTE_TYPE, ALIGNMENTS_PERSON)) {
            return EntityType.PERSON;
        }
        if (hasProperty(attributes, ATTRIBUTE_TYPE, ALIGNMENTS_ORGANISATION)) {
            return EntityType.COMPANY;
        }
        return EntityType.OTHER;
    }

    @Override
    public EntityStatus extractStatus(Map<String, List<String>> attributes) {
        // Nothing can be told about this entity
        if (attributes.size() == 0) {
            return EntityStatus.DEAD;
        }
        for (String attribute : DEAD_PERSON_ATTRIBUTES) {
            if (attributes.containsKey(attribute)) {
                return EntityStatus.DEAD;
            }
        }
        for (String attribute : DEAD_COMPANY_ATTRIBUTES) {
            if (attributes.containsKey(attribute)) {
                return EntityStatus.DEAD;
            }
        }
        return EntityStatus.ALIVE;
    }
}
