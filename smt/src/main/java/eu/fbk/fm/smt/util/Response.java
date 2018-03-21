package eu.fbk.fm.smt.util;

import com.google.gson.Gson;

import javax.annotation.Nullable;

/**
 * A common JSON response for the API
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Response {
    private static final Gson gson = new Gson();

    public static final int SUCCESS = 0;
    public static final String SUCCESS_MESSAGE = "ok";
    public static final int GENERIC_ERROR = -1;
    public static final int INVALID_PARAMS_ERROR = -2;
    public static final int NOT_FOUND = -3;
    public static final String INVALID_PARAMS_ERROR_MESSAGE = "invalid parameters";

    public int code;
    public String message;
    public Object data;

    public static Response success(@Nullable Object data) {
        Response response = new Response();
        response.data = data;
        response.code = SUCCESS;
        response.message = SUCCESS_MESSAGE;
        return response;
    }

    public static Response error(String error) {
        Response response = new Response();
        response.data = null;
        response.code = GENERIC_ERROR;
        response.message = error;
        return response;
    }

    public static Response notFound(String entity) {
        Response response = new Response();
        response.data = null;
        response.code = NOT_FOUND;
        response.message = entity+" wasn't found";
        return response;
    }

    public String respond() {
        return gson.toJson(this);
    }
}