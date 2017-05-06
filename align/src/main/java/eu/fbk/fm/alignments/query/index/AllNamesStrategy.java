package eu.fbk.fm.alignments.query.index;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;

import java.util.*;

/**
 * Strategy with a good amount of candidates that tries to include as many names as possible
 */
public class AllNamesStrategy implements QueryAssemblyStrategy {

    @Override
    public String getQuery(DBpediaResource resource) {
        Map<String, Integer> names = complileListOfNames(resource);
        if (names.size() == 0) {
            return getCleanedUpName(resource.getCleanResourceId());
        }

        if (names.size() == 1) {
            return getCleanedUpName(names.keySet().iterator().next());
        }

        StringBuilder query = new StringBuilder();
        for (String name : names.keySet()) {
            if (query.length() > 0) {
                query.append(") | (");
            }

            query.append(getCleanedUpName(name));
        }
        return "(" + query.toString() + ")";
    }

    private Map<String, Integer> complileListOfNames(DBpediaResource resource) {
        List<String> names = resource.getNames();
        List<String> givenNames = resource.getGivenNames();
        List<String> surnames = resource.getSurnames();
        String cleanId = resource.getCleanResourceId();

        if (cleanId.length() > 0) {
            names.add(cleanId);
        }

        Map<String, Integer> counts = new HashMap<>();
        boolean isPerson = resource.isPerson();
        for (String name : names) {
            name = name.trim();
            if (name.length() < 3) {
                continue;
            }

            if (isPerson && name.contains(", ")) {
                String[] parts = name.split(", ");
                if (parts.length > 2) {
                    continue;
                }

                name = parts[1] + " " + parts[0];
            }

            if (givenNames.contains(name) || surnames.contains(name)) {
                continue;
            }

            counts.put(name, counts.getOrDefault(name, 0) + 1);
        }

        return counts;
    }

    private static String getCleanedUpName(String name) {
        StringBuilder sb = new StringBuilder();
        boolean whitespace = false;
        sb.append("'");
        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (Character.isWhitespace(ch) || ch == '\'') {
                if (!whitespace) {
                    whitespace = true;
                    sb.append(" ");
                }
                continue;
            }

            whitespace = false;
            sb.append(ch);
        }
        sb.append("'");

        return sb.toString();
    }
}
