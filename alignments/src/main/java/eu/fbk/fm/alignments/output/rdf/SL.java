package eu.fbk.fm.alignments.output.rdf;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public final class SL {

    /** Recommended prefix for the vocabulary namespace: "sl". */
    public static final String PREFIX = "sl";

    /** Vocabulary namespace: "http://sociallink.futuro.media/ontology#". */
    public static final String NAMESPACE = "http://sociallink.futuro.media/ontology#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    // Classes

    /** Class sl:Candidate. */
    public static final IRI CANDIDATE_CLASS = createIRI("Candidate");

    // Object properties

    /** Object property sl:candidate. */
    public static final IRI CANDIDATE_PROPERTY = createIRI("candidate");

    /** Object property sl:account. */
    public static final IRI ACCOUNT = createIRI("account");

    // Datatype properties

    /** Datatype property sl:rank. */
    public static final IRI RANK = createIRI("rank");

    /** Datatype property sl:confidence. */
    public static final IRI CONFIDENCE = createIRI("confidence");

    // Utility methods

    private static IRI createIRI(final String localName) {
        return SimpleValueFactory.getInstance().createIRI(NAMESPACE, localName);
    }

    private SL() {
    }

}
