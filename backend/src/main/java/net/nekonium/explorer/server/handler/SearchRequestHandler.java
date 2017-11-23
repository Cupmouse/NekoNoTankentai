package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SearchRequestHandler implements RequestHandler {

    @Override
    public boolean isLackingParameter(Object jsonContent)  {
        final JSONObject jsonObjectContent = (JSONObject) jsonContent;

        return !jsonObjectContent.has("word");
    }

    @Override
    public JSONArray handle(Object jsonContent) throws Exception {
        final JSONObject jsonObjectContent = (JSONObject) jsonContent;
        // TODO this thing is in WIP, now it's just checks if entered search word exists on the database
        // TODO maybe search a name tag of an address, a transaction and a contract, or a token name
        // TODO needs simple address or transaction format check, for saving database resources
        final JSONArray jsonArrayResult = new JSONArray();

        final String searchWord = jsonObjectContent.getString("word");

        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            if (searchWord.length() == 64 + 2) {
                /* Might be a transaction */

                final PreparedStatement prpstmt = connection.prepareStatement("SELECT 1 FROM transactions WHERE hash = UNHEX(?) LIMIT 1");
                prpstmt.setString(1, searchWord.substring(2));

                final ResultSet resultSet = prpstmt.executeQuery();

                if (resultSet.next()) {
                    final JSONArray jsonArrayTx = new JSONArray();
                    jsonArrayTx.put("transaction");
                    jsonArrayResult.put(jsonArrayTx);
                }

                prpstmt.close();
            } else if (searchWord.length() == 40 + 2) {
                /* Might be an address */

                final String hexWithoutPrefix = searchWord.substring(2);

                /* Search for normal address first */
                final PreparedStatement prpstmt1 = connection.prepareStatement("SELECT 1 FROM transactions WHERE `to` = UNHEX(?) OR `from` = UNHEX(?) LIMIT 1");
                prpstmt1.setString(1, hexWithoutPrefix);
                prpstmt1.setString(2, hexWithoutPrefix);

                final ResultSet resultSet = prpstmt1.executeQuery();

                if (resultSet.next()) {
                    // Found it
                    final JSONArray jsonArrayAddr = new JSONArray();
                    jsonArrayAddr.put("normal_address");
                    jsonArrayResult.put(jsonArrayAddr);
                }

                prpstmt1.close();

                /* Search for a contract address, a contract address is created when a contract creation transaction had been executed */

                final PreparedStatement prpstmt2 = connection.prepareStatement("SELECT 1 FROM transactions WHERE contract_address = UNHEX(?) LIMIT 1");
                prpstmt2.setString(1, hexWithoutPrefix);

                final ResultSet resultSet2 = prpstmt2.executeQuery();

                if (resultSet2.next()) {
                    final JSONArray jsonArrayAddr = new JSONArray();
                    jsonArrayAddr.put("contract_address");
                    jsonArrayResult.put(jsonArrayAddr);
                }
            }
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred when closing a database connection", e);
                }
            }
        }

        return jsonArrayResult;
    }

    @Override
    public RequestContentType getContentType() {
        return RequestContentType.OBJECT;
    }
}
