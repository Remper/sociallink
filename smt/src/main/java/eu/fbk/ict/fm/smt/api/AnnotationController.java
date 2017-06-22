package eu.fbk.ict.fm.smt.api;

import com.restfb.FacebookClient;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.TextScorer;
import eu.fbk.ict.fm.smt.model.Score;
import eu.fbk.ict.fm.smt.model.ScoreBundle;
import eu.fbk.ict.fm.smt.services.*;
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

    @Inject
    KBAccessService kbAccessService;

    @Inject
    AlignmentsService alignmentsService;

    @Inject
    MLService mlService;

    //@Inject
    //FacebookClient facebookClient;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("ner")
    public String annotateNerClass(@QueryParam("text") String text) {
        List<CoreLabel> labels = annotationService.annotate(text).get(CoreAnnotations.TokensAnnotation.class);
        if (labels.size() == 0) {
            return new InvalidAttributeResponse("text").respond();
        }

        List<Annotation> response = new LinkedList<>();
        Annotation last = null;
        for (CoreLabel label : labels) {
            if (last != null && isAllowedType(label.ner()) && last.nerClass.equals(label.ner())) {
                last.token += " " + label.word();
            } else {
                last = new Annotation(label.word(), label.ner());
                response.add(last);
            }
        }
        return Response.success(response).respond();
    }

    private List<String> errorsForFinalStage(String token, String ner, String text) {
        List<String> errors = new LinkedList<>();
        if (text.length() < 5 || (token != null && text.length() < token.length())) {
            errors.add("text");
        }
        if (token == null || token.length() < 5) {
            errors.add("token");
        }
        if (ner == null) {
            errors.add("ner");
        } else {
            ner = ner.toUpperCase();
            if (!isAllowedType(ner)) {
                errors.add("ner");
            }
        }
        return errors;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("twitter")
    public String annotateWithTwitter(@QueryParam("token") String token, @QueryParam("ner") String ner, @QueryParam("text") String text) {
        List<String> errors = errorsForFinalStage(token, ner, text);
        if (errors.size() > 0) {
            return new InvalidAttributeResponse(errors).respond();
        }

        DBpediaResource resource = toResource(new Annotation(token, ner), text);
        List<User> candidates = onlineAlignmentsService.populateCandidates(resource);

        SingleAnnotation response = new SingleAnnotation();
        response.candidates = new HashMap<>();
        for (User candidate : candidates) {
            response.candidates.put(candidate.getScreenName(), candidate);
        }
        response.token = token;
        response.nerClass = ner;
        response.results = onlineAlignmentsService.compare(resource, candidates);

        return Response.success(response).respond();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("twitter/simple")
    public String annotateWithTwitterSimple(@QueryParam("token") String token, @QueryParam("ner") String ner, @QueryParam("text") String text) {
        List<String> errors = errorsForFinalStage(token, ner, text);
        if (errors.size() > 0) {
            return new InvalidAttributeResponse(errors).respond();
        }

        DBpediaResource resource = toResource(new Annotation(token, ner), text);
        List<User> candidates = onlineAlignmentsService.populateCandidates(resource);
        Iterator<User> candidatesIterator = candidates.iterator();
        Iterator<Score> scores = onlineAlignmentsService.compareWithDefault(resource, candidates).iterator();
        List<SimpleAnnotation> annotations = new LinkedList<>();

        while (candidatesIterator.hasNext()) {
            Score score = null;
            if (scores.hasNext()) {
                score = scores.next();
            }
            
            annotations.add(new SimpleAnnotation(candidatesIterator.next(), score));
        }

        return Response.success(annotations).respond();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("is_similar")
    public String isProfileSimilar(@QueryParam("resource") String resourceId, @QueryParam("uid") Long uid) throws IOException {
        List<String> errors = new LinkedList<>();
        if (uid == null || uid <= 0) {
            errors.add("uid");
        }
        if (resourceId == null || resourceId.length() < 12) {
            errors.add("resource");
        }
        if (errors.size() > 0) {
            return new InvalidAttributeResponse(errors).respond();
        }

        DBpediaResource resource = kbAccessService.getResource(resourceId);
        User user = alignmentsService.getUserById(uid);

        double score = 0.0d;
        if (user != null) {
            TextScorer scorer = new TextScorer(mlService.getDefaultScorer()).all();
            score = scorer.getFeature(user, resource);
        }

        return Response.success(score).respond();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("facebook")
    public String annotateWithFacebook(@QueryParam("token") String token, @QueryParam("ner") String ner, @QueryParam("text") String text) {
        List<String> errors = errorsForFinalStage(token, ner, text);
        if (errors.size() > 0) {
            return new InvalidAttributeResponse(errors).respond();
        }
        return "";
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String annotatePipeline(@QueryParam("text") String text) throws IOException, URISyntaxException {
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
        List<User> candidates = onlineAlignmentsService.populateCandidates(tokenResource);
        users.addAll(candidates);
        Alignment alignment = new Alignment();
        alignment.candidates = onlineAlignmentsService.produceAlignment(tokenResource, candidates);
        alignment.query = onlineAlignmentsService.getQuery(tokenResource);
        annotation.alignment = alignment;
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

    private static class SimpleAnnotation {
        public User user;
        public Score score;

        public SimpleAnnotation(User user, Score score) {
            this.user = user;
            this.score = score;
        }
    }

    private static class SingleAnnotation {
        public Map<String, User> candidates;
        public String token;
        public String nerClass;
        public ScoreBundle[] results;
    }

    private static class Alignment {
        public String query;
        public ScoreBundle candidates;
    }
}
