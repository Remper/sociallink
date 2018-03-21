package eu.fbk.fm.smt.services;

import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Properties;

/**
 * Uses stanford NLP to annotate text and find entities
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Service @Singleton
public class AnnotationService {
    private StanfordCoreNLP pipeline;

    @Inject
    public AnnotationService() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
        pipeline = new StanfordCoreNLP(props);
    }

    public Annotation annotate(String text) {
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        return document;
    }
}
