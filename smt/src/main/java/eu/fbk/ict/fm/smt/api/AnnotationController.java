package eu.fbk.ict.fm.smt.api;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.ict.fm.smt.services.AnnotationService;
import eu.fbk.ict.fm.smt.services.OnlineAlignmentsService;
import eu.fbk.ict.fm.smt.services.WikimachineService;
import eu.fbk.ict.fm.smt.util.InvalidAttributeResponse;
import eu.fbk.ict.fm.smt.util.Response;
import twitter4j.User;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * A proxy between thewikimachine endpoint and client app, enriching the data along the way
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Path("annotate")
public class AnnotationController {
    private static final String[] allowedTypes = {"ORGANIZATION", "PERSON"};

    @Inject
    WikimachineService wikimachineService;

    @Inject
    AnnotationService annotationService;

    @Inject
    OnlineAlignmentsService onlineAlignmentsService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getAvailableDatasets(@QueryParam("text") String text) throws IOException, URISyntaxException {
        List<CoreLabel> labels = annotationService.annotate(text).get(CoreAnnotations.TokensAnnotation.class);
        if (labels.size() == 0) {
            return new InvalidAttributeResponse("text").respond();
        }

        AnnotationResponse response = new AnnotationResponse();
        response.annotations = new LinkedList<>();
        response.users = new HashSet<>();
        Annotation last = null;
        for (CoreLabel label : labels) {
            if (last != null && isAllowedType(label.ner()) && last.nerClass.equals(label.ner())) {
                last.token += " " + label.word();
            } else {
                processAlignment(last, response.users, text);
                last = new Annotation(label.word(), label.ner());
                response.annotations.add(last);
            }
        }
        processAlignment(last, response.users, text);
        return Response.success(response).respond();
    }

    private boolean isAllowedType(String type) {
        for (String trueType : allowedTypes) {
            if (trueType.equals(type)) {
                return true;
            }
        }
        return false;
    }

    private void processAlignment(Annotation annotation, Set<User> users, String text) {
        if (annotation == null || !isAllowedType(annotation.nerClass)) {
            return;
        }

        DBpediaResource tokenResource = toResource(annotation, text);
        Map<User, Double> prediction = onlineAlignmentsService.produceAlignment(tokenResource);
        Alignment alignment = new Alignment();
        alignment.candidates = convertPredictionsToResult(prediction, users);
        alignment.query = onlineAlignmentsService.getQuery(tokenResource);
        annotation.alignment = alignment;
    }

    private static Map<String, Double> convertPredictionsToResult(Map<User, Double> prediction, Set<User> users) {
        Map<String, Double> result = new HashMap<>();
        for (Map.Entry<User, Double> entry : prediction.entrySet()) {
            users.add(entry.getKey());
            result.put(entry.getKey().getScreenName(), entry.getValue());
        }
        return result;
    }

    private static DBpediaResource toResource(Annotation annotation, String text) {
        Map<String, List<String>> attributes = new HashMap<>();
        attributes.put(DBpediaResource.ATTRIBUTE_NAME, Collections.singletonList(annotation.token));
        attributes.put(DBpediaResource.COMMENT_PROPERTY, Collections.singletonList(text));
        switch (annotation.nerClass) {
            default:
            case "PERSON":
                attributes.put(DBpediaResource.ATTRIBUTE_TYPE, Collections.singletonList(DBpediaResource.TYPE_PERSON));
                break;
            case "ORGANIZATION":
                attributes.put(DBpediaResource.ATTRIBUTE_TYPE, Collections.singletonList(DBpediaResource.TYPE_ORGANISATION));
                break;
        }
        return new DBpediaResource("http://fake.db.futuro.media/"+annotation.token.replace(' ', '_'), attributes);
    }

    private static class AnnotationResponse {
        public Set<User> users;
        public List<Annotation> annotations;
    }

    private static class Annotation {
        public String token;
        public String nerClass;
        public Alignment alignment;

        public Annotation(String token, String nerClass) {
            this.token = token;
            this.nerClass = nerClass;
            this.alignment = null;
        }
    }

    private static class Alignment {
        public String query;
        public Map<String, Double> candidates;
    }
}
