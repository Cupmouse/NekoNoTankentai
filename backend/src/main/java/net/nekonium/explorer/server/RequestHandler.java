package net.nekonium.explorer.server;

import net.nekonium.explorer.server.handler.BlockRequestHandler;
import org.json.JSONObject;

public interface RequestHandler<T> {

    T parseParameters(JSONObject jsonObject) throws InvalidRequestException;

    /**
     * Handling a request from {@link net.nekonium.explorer.server.endpoint.RequestEndPoint}
     * @param parameters Request's parsed parameters
     * @return Result, maybe {@link JSONObject} or {@link org.json.JSONArray}
     */
    Object handle(T parameters) throws Exception;
}
