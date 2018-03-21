package eu.fbk.fm.smt.filters;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Explicitly specifies an encoding of the response
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class EncodingResponseFilter implements ContainerResponseFilter {

    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {
        MediaType mediaType = responseContext.getMediaType();
        responseContext.getHeaders().putSingle("Content-Type", mediaType.withCharset("utf-8").toString());
    }
}
