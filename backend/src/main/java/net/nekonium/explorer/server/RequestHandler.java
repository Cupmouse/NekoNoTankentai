package net.nekonium.explorer.server;

import org.json.JSONObject;

public interface RequestHandler {

    /**
     * Handling a request from {@link net.nekonium.explorer.server.endpoint.RequestEndPoint}
     * @param content Request's content
     * @return Result, {@link JSONObject} or {@link org.json.JSONArray}
     */
    Object handle(JSONObject content) throws Exception;

}
