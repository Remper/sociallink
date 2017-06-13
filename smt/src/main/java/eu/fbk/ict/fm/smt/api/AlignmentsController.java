package eu.fbk.ict.fm.smt.api;

import eu.fbk.fm.alignments.index.db.tables.records.AlignmentsRecord;
import eu.fbk.ict.fm.smt.services.AlignmentsService;
import eu.fbk.ict.fm.smt.services.KBAccessService;
import eu.fbk.ict.fm.smt.util.InvalidAttributeResponse;
import eu.fbk.ict.fm.smt.util.Response;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.*;

/**
 * Retrieve an alignment records from the DB
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Path("alignments")
public class AlignmentsController {
    @Inject
    AlignmentsService alignments;

    @Inject
    KBAccessService kbService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("by_twitter_id")
    public String getAlignmentsByTwitterID(@QueryParam("id") Long id) {
        if (id == null || id <= 0) {
            return new InvalidAttributeResponse("id").respond();
        }

        return getResultById(id);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("by_resource_uri")
    public String getAlignmentsByResourceId(@QueryParam("uri") String uri) {
        if (uri == null || uri.length() <= 3) {
            return new InvalidAttributeResponse("uri").respond();
        }

        List<AlignmentsRecord> records = alignments.getRecordsByResourceId(uri);
        ResourceResult result = new ResourceResult();
        result.request = uri;
        result.alignment = null;
        result.candidates = new TwitterEntity[records.size()];
        int order = 0;
        for (AlignmentsRecord record : records) {
            TwitterEntity entity = new TwitterEntity();
            entity.uid = record.getUid();
            entity.score = record.getScore();
            if (record.getIsAlignment()) {
                result.alignment = entity.uid;
            }
            result.candidates[order] = entity;
            order++;
        }
        return Response.success(result).respond();
    }

    private String getResultById(Long id) {
        List<AlignmentsRecord> records = alignments.getRecordsByTwitterId(id);
        //Sort by score in descending order
        records.sort((o1, o2) -> {
            double o1v = Double.valueOf(o1.getScore());
            double o2v = Double.valueOf(o2.getScore());

            return Double.compare(o2v, o1v);
        });

        TwitterResult result = new TwitterResult();
        result.request = id;
        result.alignment = null;
        result.type = null;
        result.candidates = new ResourceEntity[records.size()];
        int order = 0;
        HashMap<String, Integer> types = new HashMap<>();
        //Add all the entities to the response, figure out the alignment
        for (AlignmentsRecord record : records) {
            ResourceEntity entity = new ResourceEntity();
            entity.resourceId = record.getResourceId();
            entity.score = record.getScore();
            entity.type = kbService.getType(entity.resourceId);
            if (entity.score > 0) {
                types.put(entity.type, types.getOrDefault(entity.type, 0)+1);
            }
            if (record.getIsAlignment()) {
                result.alignment = entity.resourceId;
                result.type = entity.type;
            }
            result.candidates[order] = entity;
            order++;
        }

        //If there is no alignment — at least provide a possible type
        if (result.alignment == null) {
            String majorType = null;
            int majorTypeNum = 0;
            for (Map.Entry<String, Integer> type : types.entrySet()) {
                if (type.getValue() > majorTypeNum) {
                    majorType = type.getKey();
                    majorTypeNum = type.getValue();
                } else if (type.getValue() == majorTypeNum) {
                    majorType = null;
                }
            }
            result.type = majorType;
        }
        return Response.success(result).respond();
    }

    private static class TwitterEntity {
        private long uid;
        private float score;
    }

    private static class ResourceEntity {
        private String resourceId;
        private String type;
        private float score;
    }

    private static class TwitterResult {
        private Long request;
        private String alignment;
        private String type;
        private ResourceEntity[] candidates;
    }

    private static class ResourceResult {
        private String request;
        private Long alignment;
        private boolean is_dead;
        private TwitterEntity[] candidates;
    }
}
