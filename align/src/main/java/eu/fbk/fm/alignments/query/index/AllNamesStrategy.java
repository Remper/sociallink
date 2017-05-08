package eu.fbk.fm.alignments.query.index;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Strategy with a good amount of candidates that tries to include as many names as possible
 */
public class AllNamesStrategy implements QueryAssemblyStrategy {

    private static final int NAMES_THRESHOLD = 3;

    @Override
    public String getQuery(DBpediaResource resource) {
        Map<String, Integer> names = complileListOfNames(resource);
        if (names.size() == 0) {
            return getCleanedUpName(resource.getCleanResourceId());
        }

        if (names.size() == 1) {
            return getCleanedUpName(names.keySet().iterator().next());
        }

        LinkedList<Map.Entry<String, Integer>> sortedNames = new LinkedList<>(names.entrySet());
        Comparator<Map.Entry<String, Integer>> comparator = Comparator.comparing(Map.Entry::getValue);
        sortedNames.sort(comparator.reversed());

        int remaining = NAMES_THRESHOLD;
        StringBuilder query = new StringBuilder();
        for (Map.Entry<String, Integer> name : sortedNames) {
            if (query.length() > 0) {
                query.append(") | (");
            }

            query.append(getCleanedUpName(name.getKey()));
            if (--remaining == 0) {
                break;
            }
        }
        return "(" + query.toString() + ")";
    }

    private Map<String, Integer> complileListOfNames(DBpediaResource resource) {
        List<String> names = resource.getNames();
        names.addAll(resource.getProperty(DBpediaResource.ATTRIBUTE_LABEL));
        List<String> givenNames = lowercaseNames(resource.getGivenNames());
        List<String> surnames = lowercaseNames(resource.getSurnames());
        String cleanId = resource.getCleanResourceId();

        if (cleanId.length() > 0 && !cleanId.matches("Q[0-9]+")) {
            names.add(cleanId);
        }

        Map<String, Integer> counts = new HashMap<>();
        boolean isPerson = resource.isPerson();
        for (String name : names) {
            name = name.trim().toLowerCase();
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

    private static List<String> lowercaseNames(List<String> names) {
        return names.stream().map(String::toLowerCase).collect(Collectors.toList());
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
