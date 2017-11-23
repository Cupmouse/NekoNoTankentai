package net.nekonium.explorer.server.endpoint;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import org.json.JSONException;
import org.json.JSONObject;

import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ServerEndpoint("/request")
public class RequestEndPoint {

    private static final int THREAD_NUMBER = 2;

    private static ExecutorService queryExecutor;
    private static final HashMap<String, RequestHandler<?>> handlers = new HashMap<>();

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

    @OnOpen
    public void onOpen(Session session) {
        session.setMaxIdleTimeout(3 * 60 * 1000);   // 3 minutes to idle time out
        session.setMaxTextMessageBufferSize(256);   // TODO set to ideal size
        session.setMaxTextMessageBufferSize(1024);  // TODO this too
    }

    @OnMessage
    public <T> void onMessage(Session session, String message) throws IOException {
        final JSONObject jsonObject;  // Deserialize json string client sent
        try {
            jsonObject = new JSONObject(message);
        } catch (JSONException e) {
            session.close();    // It's not json!
            return;
        }

        if (!jsonObject.has("id") || !jsonObject.has("demand") || !jsonObject.has("content")) {
            session.close();    // If request doesn't have either one of them, it is invalid
            return;
        }

        final long requestId = jsonObject.getLong("id"); // This request's id for identification between multiple requests
        final String demand = jsonObject.getString("demand"); // This query's demand (handler name)

        final RequestHandler<T> handler = (RequestHandler<T>) handlers.get(demand.toLowerCase());

        if (handler == null) {
            // Requested handler is not registered
            // todo freak count
            session.close(); // This client is weirdo, closing the session
            return;
        }

        /* Requested handler is registered */

        final T parameters;

        try {
            parameters = handler.parseParameters(jsonObject);
        } catch (InvalidRequestException e) {
            ExplorerServer.getInstance().getLogger().info("Error on handling request", e);
            // fixme you can print this exception if you like to debug
            session.close();    // Invalid content format
            return;
        }

        /* Call handler from another thread */

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
                jsonResult = handler.handle(parameters);
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
    public void onError(Session session, Throwable throwable) {
        if (throwable instanceof SocketTimeoutException) {
            // Socket timed out
            ExplorerServer.getInstance().getLogger().info("Session timed out for {}", session.getId());
        } else {
            ExplorerServer.getInstance().getLogger().error("An unknown error occurred", throwable);
        }

        /* Close the session when error occurred */
        try {
            session.close();
        } catch (IOException e) {
            ExplorerServer.getInstance().getLogger().error("An error occurred when closing a session", e);
        }
    }

    public static void registerHandler(String handlerId, RequestHandler handler) {
        if (handlers.containsKey(handlerId)) {
            throw new IllegalArgumentException("The request handler with same id exists");
        }

        handlers.put(handlerId.toLowerCase(), handler);
    }
}
