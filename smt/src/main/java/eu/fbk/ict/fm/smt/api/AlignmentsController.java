package eu.fbk.ict.fm.smt.api;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.AlignmentsRecord;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.ResourcesRecord;
import eu.fbk.ict.fm.smt.services.AlignmentsService;
import eu.fbk.ict.fm.smt.util.InvalidAttributeResponse;
import eu.fbk.ict.fm.smt.util.Response;
import org.jooq.Record2;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Retrieve an alignment records from the DB
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Path("alignments")
public class AlignmentsController {
    @Inject
    AlignmentsService alignments;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("datasets")
    public String getAvailableDatasets() {
        return Response
            .success(alignments
                .getAvailableDatasets()
                .stream()
                .map(Dataset::new)
                .collect(Collectors.toList())
            )
            .respond();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("by_twitter_id")
    public String getAlignmentsByTwitterID(@QueryParam("id") Long id, @QueryParam("whitelist") String whitelist) {
        if (id == null || id <= 0) {
            return new InvalidAttributeResponse("id").respond();
        }

        return getResultById(id, getWhitelistFromParameter(whitelist));
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("by_twitter_username")
    public String getAlignmentsByTwitterUsername(
            @QueryParam("username") String username,
            @QueryParam("whitelist") String whitelist) {

        if (username == null || username.length() <= 3) {
            return new InvalidAttributeResponse("username").respond();
        }

        Long id = alignments.getIdByUsername(username);
        if (id == null) {
            return Response.notFound("username").respond();
        }
        return getResultById(id, getWhitelistFromParameter(whitelist));
    }

    private Collection<String> getWhitelistFromParameter(String whitelist) {
        Collection<String> whitelistArr = tryConstructWhitelistArray(whitelist);
        if (whitelistArr == null && whitelist != null && whitelist.length() > 0) {
            whitelistArr = new LinkedList<String>() {{
                add(whitelist);
            }};
        }
        return whitelistArr;
    }

    private Collection<String> tryConstructWhitelistArray(String whitelist) {
        try {
            String[] whitelistArr = new Gson().fromJson(whitelist, String[].class);
            if (whitelistArr == null) {
                return null;
            }
            return Arrays.stream(whitelistArr).collect(Collectors.toList());
        } catch(JsonSyntaxException e) {
            return null;
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("by_resource_uri")
    public String getAlignmentsByResourceId(@QueryParam("uri") String uri) {
        if (uri == null || uri.length() <= 3) {
            return new InvalidAttributeResponse("uri").respond();
        }

        ResourcesRecord resource = alignments.getResourceById(uri);
        if (resource == null) {
            return Response.notFound("uri").respond();
        }

        List<AlignmentsRecord> records = alignments.getRecordsByResourceId(uri);
        ResourceResult result = new ResourceResult();
        result.request = uri;
        result.alignment = null;
        result.dataset = resource.getDataset();
        result.is_dead = resource.getIsDead() == 1;
        result.candidates = new TwitterEntity[records.size()];
        int order = 0;
        for (AlignmentsRecord record : records) {
            TwitterEntity entity = new TwitterEntity();
            entity.twitterId = record.getTwitterId();
            entity.score = Double.valueOf(record.getScore());
            if (record.getIsAlignment() != 0) {
                result.alignment = entity.twitterId;
            }
            result.candidates[order] = entity;
            order++;
        }
        return Response.success(result).respond();
    }

    private String getResultById(Long id, Collection<String> whitelist) {
        List<AlignmentsRecord> records = alignments.getRecordsByTwitterId(id, whitelist);
        TwitterResult result = new TwitterResult();
        result.request = id;
        result.alignment = null;
        result.candidates = new ResourceEntity[records.size()];
        int order = 0;
        for (AlignmentsRecord record : records) {
            ResourceEntity entity = new ResourceEntity();
            entity.resourceId = record.getResourceId();
            entity.score = Double.valueOf(record.getScore());
            if (record.getIsAlignment() != 0) {
                result.alignment = entity.resourceId;
            }
            result.candidates[order] = entity;
            order++;
        }
        return Response.success(result).respond();
    }

    private static class Dataset {
        private String name;
        private Integer count;

        private Dataset(Record2<String, Integer> record) {
            name = record.value1();
            count = record.value2();
        }
    }

    private static class TwitterEntity {
        private long twitterId;
        private double score;
    }

    private static class ResourceEntity {
        private String resourceId;
        private double score;
    }

    private static class TwitterResult {
        private Long request;
        private String alignment;
        private ResourceEntity[] candidates;
    }

    private static class ResourceResult {
        private String request;
        private Long alignment;
        private String dataset;
        private boolean is_dead;
        private TwitterEntity[] candidates;
    }
}
