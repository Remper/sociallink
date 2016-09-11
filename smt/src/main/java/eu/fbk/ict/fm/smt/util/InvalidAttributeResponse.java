package eu.fbk.ict.fm.smt.util;

import java.util.Collection;

/**
 * Response with invalid parameters
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class InvalidAttributeResponse extends Response {
    public String[] invalid;

    public InvalidAttributeResponse(Collection<String> invalid) {
        init();
        this.invalid = new String[invalid.size()];
        int i = 0;
        for (String invAttr : invalid) {
            this.invalid[i] = invAttr;
            i++;
        }
    }

    public InvalidAttributeResponse(String invalid) {
        this(new String[]{invalid});
    }

    public InvalidAttributeResponse(String[] invalid) {
        init();
        this.invalid = invalid;
    }

    private void init() {
        this.code = INVALID_PARAMS_ERROR;
        this.message = INVALID_PARAMS_ERROR_MESSAGE;
        this.data = null;
    }
}