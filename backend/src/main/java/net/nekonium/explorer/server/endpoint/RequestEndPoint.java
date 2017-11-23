package net.nekonium.explorer.server.endpoint;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ServerEndpoint("/request")
public class RequestEndPoint {

    private static final int THREAD_NUMBER = 2;

    private static ExecutorService queryExecutor;
    private static final HashMap<String, RequestHandler> handlers = new HashMap<>();

    public static void initQueryExecutor() {
        queryExecutor = Executors.newFixedThreadPool(THREAD_NUMBER);
    }

    public static void shutdownQueryExecutorAndWait() {
        queryExecutor.shutdown();

        /* Try to shutdown the query executor, up to 10 times (10 seconds) */
        for (int i = 1; i <= 10; i++) {
            if (i == 1) {
                ExplorerServer.getInstance().getLogger().info("Waiting for query executor to shutdown...");
            } else {
                ExplorerServer.getInstance().getLogger().info("Waiting for query executor to shutdown... {}/10", i);
            }

            boolean terminated = false;

            try {
                terminated = queryExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                ExplorerServer.getInstance().getLogger().error("An error occurred when waiting to shutdown", e);
            }

            if (terminated) {
                break;
            }
        }
    }

    @OnMessage
    public void onMessage(Session session, String message) throws IOException {
        final JSONObject jsonObject = new JSONObject(message);  // Deserialize json string client sent

        final long requestId = jsonObject.getLong("id"); // This request's id for identification between multiple requests
        final String demand = jsonObject.getString("demand"); // This query's demand (handler name)
        final JSONObject jsonObjectContent = jsonObject.getJSONObject("content"); // This request's content

        final RequestHandler handler = handlers.get(demand.toLowerCase());

        if (handler == null) {
            // Requested handler is not registered
            // todo freak count
            session.close(); // This client is weirdo, closing the session
            return;
        }

        /* Requested handler is registered, do it on another thread */

        final WeakReference<Session> sessionWeakReference = new WeakReference<>(session);   // Wrap an session with WeakReference for avoiding memory leak (maybe)

        queryExecutor.submit(() -> {
            final Session sessionRef = sessionWeakReference.get();

            if (sessionRef == null) {
                // An session instance is already cleared by gc
                return;
            }

            /* let handler do their job */

            final Object jsonResult;

            try {
                jsonResult = handler.handle(jsonObjectContent);
            } catch (InvalidRequestException e) {   // This is one special exception, don't show print error on the logger
                /* Received request was invalid, closing the session */
                try {
                    session.close();
                } catch (IOException e1) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred when closing a session", e1);
                }
                return;
            } catch (Exception e) {
                /* Something went wrong in handler, not sure what had happened, but print it on the logger and close the connection for safety */
                ExplorerServer.getInstance().getLogger().error("An error occurred when processing a request with handler [{}]", demand, e);
                try {
                    session.close();
                } catch (IOException e1) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred when closing a session", e1);
                }
                return;
            }

            /* Wrap handler's result */

            final JSONObject jsonObjectWrapper = new JSONObject();

            jsonObjectWrapper.put("id", requestId);
            jsonObjectWrapper.put("content", jsonResult);

            sessionRef.getAsyncRemote().sendText(jsonObjectWrapper.toString());    // Sending asynchronous
        });
    }

    @OnError
    public void onError(Session session) {
        try {
            session.close();
        } catch (IOException e) {
            ExplorerServer.getInstance().getLogger().error("An error occurred when closing a session", e);
        }
    }
}
