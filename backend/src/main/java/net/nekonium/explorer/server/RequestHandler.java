package net.nekonium.explorer.server;

import org.json.JSONObject;

public interface RequestHandler {

    boolean isLackingParameter(Object jsonContent);

    /**
     * Handling a request from {@link net.nekonium.explorer.server.endpoint.RequestEndPoint}
     * @param jsonContent Request's content
     * @return Result, {@link JSONObject} or {@link org.json.JSONArray}
     */
    Object handle(Object jsonContent) throws Exception;

    /**
     *
     * @return This handler's content type either JSONObject or JSONArray
     */
    RequestContentType getContentType();

    enum RequestContentType {
        OBJECT, ARRAY
    }
}
