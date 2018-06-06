package eu.fbk.fm.smt.model;

import eu.fbk.fm.alignments.scorer.UserData;
import twitter4j.User;

import java.util.*;

/**
 * A set of named candidates
 */
public class CandidatesBundle {
    public final String type;
    public int size;
    private final List<String> candidates;

    public CandidatesBundle(String type, List<String> candidates) {
        this.type = type;
        this.candidates = candidates;
        this.size = candidates.size();
    }

    public CandidatesBundle(String type) {
        this(type, new LinkedList<>());
    }

    public void addCandidate(String user) {
        candidates.add(user);
        size++;
    }

    public Iterator<String> getCandidates() {
        return candidates.iterator();
    }

    public static class Resolved {
        public final Map<String, UserData> dictionary = new HashMap<>();
        public final CandidatesBundle bundle;

        public Resolved(CandidatesBundle bundle) {
            this.bundle = bundle;
        }

        public void addUser(UserData user) {
            bundle.addCandidate(user.getScreenName());
            dictionary.put(user.getScreenName(), user);
        }
    }

    public static Resolved resolved(String type) {
        return new Resolved(new CandidatesBundle(type));
    }
}