package eu.fbk.fm.alignments;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Simple property map associated with resource identifier
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class DBpediaResource {
    private static final String DBPEDIA_ONTOLOGY = "http://dbpedia.org/ontology/";
    private static final String DBPEDIA_PROPERTY = "http://dbpedia.org/property/";
    private static final String WIKIDATA_ENTITY = "http://www.wikidata.org/entity/";

    public static final String COMMENT_PROPERTY = "http://www.w3.org/2000/01/rdf-schema#comment";
    public static final String ABSTRACT_PROPERTY = DBPEDIA_ONTOLOGY + "abstract";

    public static final String ATTRIBUTE_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    public static final String ATTRIBUTE_NAME = "http://xmlns.com/foaf/0.1/name";

    public static final String TYPE_PERSON = DBPEDIA_ONTOLOGY + "Person";
    public static final String TYPE_ORGANISATION = DBPEDIA_ONTOLOGY + "Organisation";
    public static final String TYPE_COMPANY = DBPEDIA_ONTOLOGY + "Company";

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

    private String identifier;
    private Map<String, List<String>> attributes;

    public DBpediaResource(String identifier, Map<String, List<String>> attributes) {
        this.identifier = identifier;
        this.attributes = attributes;
    }

    public String getIdentifier() {
        return identifier;
    }

    public Map<String, List<String>> getAttributes() {
        return attributes;
    }

    public List<String> getProperty(String property) {
        return attributes.getOrDefault(property, new LinkedList<>());
    }

    public boolean isPerson() {
        return hasProperty(ATTRIBUTE_TYPE, TYPE_PERSON);
    }

    public boolean isCompany() {
        return hasProperty(ATTRIBUTE_TYPE, TYPE_ORGANISATION) || hasProperty(ATTRIBUTE_TYPE, TYPE_COMPANY);
    }

    public boolean isOther() {
        return !(isCompany() || isPerson());
    }

    public boolean isDead() {
        for (String attribute : DEAD_PERSON_ATTRIBUTES) {
            if (attributes.containsKey(attribute)) {
                return true;
            }
        }
        for (String attribute : DEAD_COMPANY_ATTRIBUTES) {
            if (attributes.containsKey(attribute)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasProperty(String relation, String value) {
        if (!attributes.containsKey(relation)) {
            return false;
        }
        for (String type : attributes.get(relation)) {
            if (type.equals(value)) {
                return true;
            }
        }

        return false;
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

    public List<String> getNames() {
        return getProperty(ATTRIBUTE_NAME);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DBpediaResource)) {
            return super.equals(obj);
        }
        return getIdentifier().equals(((DBpediaResource) obj).getIdentifier());
    }

    @Override
    public int hashCode() {
        return getIdentifier().hashCode();
    }
}