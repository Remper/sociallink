package eu.fbk.fm.alignments.query.index;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Strategy with a good amount of candidates that tries to include as many names as possible
 */
public class AllNamesStrategy implements QueryAssemblyStrategy {

    private static final int NAMES_THRESHOLD = 3;
    private final int excludeNames;

    public AllNamesStrategy() {
        this(0);
    }

    public AllNamesStrategy(int excludeNames) {
        this.excludeNames = excludeNames;
    }

    @Override
    public String getQuery(DBpediaResource resource, int option) {
        return constructQuery(resource, this.excludeNames+option);
    }

    @Override
    public String getQuery(DBpediaResource resource) {
        return constructQuery(resource, this.excludeNames);
    }

    private String constructQuery(DBpediaResource resource, int excludeNames) {
        Map<String, Integer> names = complileListOfNames(resource);
        LinkedList<Map.Entry<String, Integer>> sortedNames = new LinkedList<>(names.entrySet());
        Comparator<Map.Entry<String, Integer>> comparator = Comparator.comparing(Map.Entry::getValue);
        sortedNames.sort(comparator.reversed());

        int remaining = min(NAMES_THRESHOLD-excludeNames, max(sortedNames.size()-excludeNames, 0));

        if (sortedNames.size() == 0) {
            return getCleanedUpName(resource.getCleanResourceId());
        }

        if (sortedNames.size() == 1 || remaining < 2) {
            return getCleanedUpName(sortedNames.get(0).getKey());
        }

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
        names.addAll(resource.getLabels());
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

        if (excludeNames > 0) {
            if (counts.size() <= excludeNames) {
                counts.clear();
                return counts;
            }

            LinkedList<String> sortedNames = new LinkedList<>(counts.keySet());
            Comparator<String> comparator = Comparator.comparing(String::length);
            sortedNames.sort(comparator);
            for (int i = 0; i < excludeNames; i++) {
                counts.remove(sortedNames.get(i));
            }
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
