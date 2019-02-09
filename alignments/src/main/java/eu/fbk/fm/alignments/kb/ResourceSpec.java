package eu.fbk.fm.alignments.kb;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Resource specification that bakes raw RDF data into a resource components
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface ResourceSpec {

    // Ontologies
    String DBPEDIA_ONTOLOGY = "http://dbpedia.org/ontology/";
    String DBPEDIA_PROPERTY = "http://dbpedia.org/property/";
    String WIKIDATA_ENTITY = "http://www.wikidata.org/entity/";
    String ALIGNMENTS_ONTOLOGY = "http://alignments.futuro.media/ontology#";
    String FOAF_ONTOLOGY = "http://xmlns.com/foaf/0.1/";
    String SCHEMA_ORG_ONTOLOGY = "http://schema.org/";
    String SKOS_ONTOLOGY = "http://www.w3.org/2004/02/skos/core#";
    String RDFS_ONTOLOGY = "http://www.w3.org/2000/01/rdf-schema#";

    // Textual data and names
    List<String> extractNames(Map<String, List<String>> attributes);
    List<String> extractLabels(Map<String, List<String>> attributes);
    List<String> extractDescriptions(Map<String, List<String>> attributes);
    List<String> extractGivenNames(Map<String, List<String>> attributes);
    List<String> extractSurnames(Map<String, List<String>> attributes);

    // Type information
    enum EntityType {
        PERSON, COMPANY, OTHER
    }

    enum EntityStatus {
        DEAD, ALIVE
    }

    EntityType extractType(Map<String, List<String>> attributes);
    EntityStatus extractStatus(Map<String, List<String>> attributes);

    // Misc methods
    static boolean hasProperty(Map<String, List<String>> attributes, String relation, String value) {
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

    static List<String> getProperty(Map<String, List<String>> attributes, String property) {
        return new LinkedList<>(attributes.getOrDefault(property, new LinkedList<>()));
    }
}
