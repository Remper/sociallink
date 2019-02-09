package eu.fbk.fm.smt.api;

import com.google.gson.JsonArray;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import eu.fbk.fm.alignments.kb.DBpediaSpec;
import eu.fbk.fm.alignments.kb.KBResource;
import eu.fbk.fm.alignments.PrepareTrainingSet;
import eu.fbk.fm.alignments.scorer.TextScorer;
import eu.fbk.fm.alignments.scorer.UserData;
import eu.fbk.fm.alignments.twitter.TwitterService;
import eu.fbk.fm.smt.model.CandidatesBundle;
import eu.fbk.fm.smt.model.Score;
import eu.fbk.fm.smt.model.ScoreBundle;
import eu.fbk.fm.smt.services.*;
import eu.fbk.fm.smt.util.InvalidAttributeResponse;
import eu.fbk.fm.smt.util.Response;
import twitter4j.User;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A proxy between thewikimachine endpoint and client app, enriching the data along the way
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Path("annotate")
public class AnnotationController {
    private static final String[] allowedTypes = {"ORGANIZATION", "PERSON"};

    @Inject
    private TwitterService twitter;

    @Inject
    WikimachineService wikimachineService;

    @Inject
    AnnotationService annotationService;

    @Inject
    OnlineAlignmentsService onlineAlign;

    @Inject
    KBAccessService kbAccessService;

    @Inject
    AlignmentsService alignmentsService;

    @Inject
    MLService mlService;

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
        if (text == null || text.length() < 5 || (token != null && text.length() < token.length())) {
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

    private String processTwitterAnnotationWithComparator(String token, String ner, String text, BiFunction<KBResource, List<User>, List<ScoreBundle>> func) {
        List<String> errors = errorsForFinalStage(token, ner, text);
        if (errors.size() > 0) {
            return new InvalidAttributeResponse(errors).respond();
        }

        KBResource resource = toResource(new Annotation(token, ner), text);
        SingleAnnotation response = new SingleAnnotation();
        response.token = token;
        response.tokenClass = ner;
        response.context = text;
        response.dictionary = new HashMap<>();

        List<CandidatesBundle.Resolved> candidates = getCandidates(resource);

        List<UserData> candidateList = candidates.stream()
            .flatMap(resolved -> resolved.dictionary.values().stream()).collect(Collectors.toList());

        candidateList.stream()
            .map(user -> user.profile)
            .forEach(cand -> response.dictionary.put(cand.getScreenName(), cand));

        //Request additional user data
        PrepareTrainingSet.fillAdditionalData(candidateList, twitter);

        response.caResults = candidates.stream().map(cand -> cand.bundle).collect(Collectors.toList());
        response.caStats = candidateList
            .stream()
            .map(candidate -> String.format(
                "[@%s] Statuses: %d",
                candidate.getScreenName(),
                candidate.get(PrepareTrainingSet.StatusesProvider.class).orElse(new JsonArray()).getAsJsonArray().size()
            )).collect(Collectors.toList());
        response.csResults = func.apply(resource, new LinkedList<>(response.dictionary.values()));

        return Response.success(response).respond();
    }

    public List<CandidatesBundle.Resolved> getCandidates(KBResource resource) {
        return new LinkedList<CandidatesBundle.Resolved>(){{
            add(onlineAlign.performCALive(resource));
            add(onlineAlign.performCASocialLink(resource));
        }};
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String annotateWithTwitter(@QueryParam("token") String token, @QueryParam("ner") String ner, @QueryParam("text") String text) {

        return processTwitterAnnotationWithComparator(
            token, ner, text,
            (resource, candidates) -> new LinkedList<ScoreBundle>() {{
                add(onlineAlign.performCSSocialLink(resource, candidates));
                addAll(onlineAlign.compare(resource, candidates));
            }}
        );
    }


    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes("application/x-www-form-urlencoded")
    public String annotateWithTwitterPost(@FormParam("token") String token, @FormParam("ner") String ner, @FormParam("text") String text) {
        return annotateWithTwitter(token, ner, text);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("simple")
    public String annotateWithTwitterSimple(@QueryParam("token") String token, @QueryParam("ner") String ner, @QueryParam("text") String text) {
        return processTwitterAnnotationWithComparator(
            token, ner, text,
            (resource, candidates) -> new LinkedList<ScoreBundle>() {{
                add(onlineAlign.compareWithDefault(resource, candidates));
            }}
        );
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

        KBResource resource = kbAccessService.getResource(resourceId);
        User user = alignmentsService.getUserById(uid);

        double score = 0.0d;
        if (user != null) {
            TextScorer scorer = new TextScorer(mlService.getDefaultScorer()).all();
            score = scorer.getFeature(user, resource);
        }

        return Response.success(score).respond();
    }

    private boolean isAllowedType(String type) {
        for (String trueType : allowedTypes) {
            if (trueType.equals(type)) {
                return true;
            }
        }
        return false;
    }

    private static KBResource toResource(Annotation annotation, String text) {
        Map<String, List<String>> attributes = new HashMap<>();
        attributes.put(DBpediaSpec.ATTRIBUTE_NAME, Collections.singletonList(annotation.token));
        attributes.put(DBpediaSpec.COMMENT_PROPERTY, Collections.singletonList(text));
        switch (annotation.nerClass) {
            default:
            case "PERSON":
                attributes.put(DBpediaSpec.ATTRIBUTE_TYPE, Collections.singletonList(DBpediaSpec.ALIGNMENTS_PERSON));
                break;
            case "ORGANIZATION":
                attributes.put(DBpediaSpec.ATTRIBUTE_TYPE, Collections.singletonList(DBpediaSpec.ALIGNMENTS_ORGANISATION));
                break;
        }
        return new KBResource("http://fake.db.futuro.media/"+annotation.token.replace(' ', '_'), new DBpediaSpec(), attributes);
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
        public String context;
        public String token;
        public String tokenClass;
        public List<CandidatesBundle> caResults;
        public List<String> caStats;
        public List<ScoreBundle> csResults;
        public Map<String, User> dictionary;
    }

    private static class Alignment {
        public String query;
        public ScoreBundle candidates;
    }
}
