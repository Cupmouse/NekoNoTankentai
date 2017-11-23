package net.nekonium.explorer.server.endpoint;

import net.nekonium.explorer.server.ExplorerServer;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.websocket.OnMessage;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ServerEndpoint("/search")
public class SearchEndPoint {

    private static final int THREAD_NUMBER = 2;
    private final ExecutorService threadPool;

    public SearchEndPoint() {
        this.threadPool = Executors.newFixedThreadPool(THREAD_NUMBER);
    }

    @OnMessage
    public void onMessage(Session session, String message) throws IOException {
        final JSONObject jsonObject = new JSONObject(message);

        final String word = jsonObject.getString("word");
        final JSONArray results = new JSONArray();

        Connection connection = null;

        try {
            connection = ExplorerServer.getBackend().getDatabaseManager().getConnection();

            if (word.length() == 64 + 2) {
                // Might be a transaction

                final PreparedStatement prpstmt = connection.prepareStatement("SELECT 1 FROM transactions WHERE hash = UNHEX(?) LIMIT 1");

                final ResultSet resultSet = prpstmt.executeQuery();

                if (resultSet.next()) {
                    final JSONArray jsonArrayTx = new JSONArray();

                }

                prpstmt.close();
                connection.close();
            } else if (word.length() == 40 + 2) {
                // Might be an address

                try {
                    final PreparedStatement prpstmt = connection.prepareStatement("SELECT 1 FROM transactions WHERE `to` = UNHEX(?) OR `from` = UNHEX(?) LIMIT 1");

                    final ResultSet resultSet = prpstmt.executeQuery();

                    if (resultSet.next()) {
                        final JSONArray jsonArrayTx = new JSONArray();
                        jsonArrayTx.put("address");
                        results.put(jsonArrayTx);
                        // Found it
                    }

                    prpstmt.close();
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }



                session.getAsyncRemote().sendText(results.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    ExplorerServer.getLogger()
                }
            }
        }
    }
}
