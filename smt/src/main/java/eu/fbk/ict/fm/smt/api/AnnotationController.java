package eu.fbk.ict.fm.smt.api;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import eu.fbk.ict.fm.smt.services.AlignmentsService;
import eu.fbk.ict.fm.smt.services.AnnotationService;
import eu.fbk.ict.fm.smt.services.KBAccessService;
import eu.fbk.ict.fm.smt.services.WikimachineService;
import eu.fbk.ict.fm.smt.util.Response;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

/**
 * A proxy between thewikimachine endpoint and client app, enriching the data along the way
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Path("annotate")
public class AnnotationController {
    @Inject
    WikimachineService wikimachineService;

    @Inject
    AnnotationService annotationService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getAvailableDatasets(@QueryParam("text") String text) throws IOException, URISyntaxException {
        List<CoreLabel> labels = annotationService.annotate(text).get(CoreAnnotations.TokensAnnotation.class);

        List<Annotation> responses = new LinkedList<>();
        Annotation last = null;
        for (CoreLabel label : labels) {
            if (last != null && !label.ner().equals("O") && last.nerClass.equals(label.ner())) {
                last.token += " " + label.word();
                last.alignment = "bsearcher";
            } else {
                last = new Annotation(label.word(), label.ner(), "bsearcher");
                responses.add(last);
            }
        }
        return Response.success(responses).respond();
    }

    public static class Annotation {
        public String token;
        public String nerClass;
        public String alignment;

        public Annotation(String token, String nerClass, String alignment) {
            this.token = token;
            this.nerClass = nerClass;
            this.alignment = alignment;
        }
    }
}
