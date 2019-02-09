package eu.fbk.fm.alignments.query;

import eu.fbk.fm.alignments.kb.KBResource;
import eu.fbk.utils.core.strings.LevenshteinDistance;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Strategy with a good amount of candidates that tries to include as many names as possible
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class NoQuotesDupesStrategy implements QueryAssemblyStrategy {

    @Override
    public String getQuery(KBResource resource) {
        String[] names = resource.getNames().toArray(new String[0]);
        StringBuilder sb = new StringBuilder();
        boolean contains = false;
        int appended = 0;
        String cleanId = resource.getCleanResourceId();
        Arrays.sort(names, Comparator.comparingInt(String::length));
        for (String name : names) {
            if (cleanId.equals(name) || new LevenshteinDistance().apply(name.trim(), cleanId.trim()) <= 1) {
                contains = true;
            }
            if (name.contains(", ") || name.trim().length() <= 3) {
                continue;
            }
            if (sb.length() + name.length() + 6 > 200) {
                continue;
            }

            appended++;
            appendName(sb, name);
        }

        if (sb.length() == 0 || !contains) {
            appended++;
            appendName(sb, cleanId);
        }

        if (appended == 1) {
            return sb.toString();
        }

        return "(" + sb.toString() + ")";
    }

    private static void appendName(StringBuilder sb, String name) {
        if (name.length() == 0) {
            return;
        }
        if (sb.length() > 0) {
            sb.append(") OR (");
        }
        sb.append(name);
    }
}
