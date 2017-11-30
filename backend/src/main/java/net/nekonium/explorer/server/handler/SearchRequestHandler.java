package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SearchRequestHandler implements RequestHandler<String> {

    @Override
    public String parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        if (jsonObject.get("content") instanceof String) {
            return jsonObject.getString("content");
        }

        throw new InvalidRequestException("Content must be string");
    }

    @Override
    public JSONArray handle(String searchWord) throws Exception {
        // TODO this thing is in WIP, now it's just checks if entered search word exists on the database
        // TODO maybe search a name tag of an address, a transaction and a contract, or a token name
        // TODO needs simple address or transaction format check, for saving database resources
        final JSONArray jsonArrayResult = new JSONArray();

        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            try {
                final BigInteger number = new BigInteger(searchWord);

                /* This continues if word was complete numeric (number) */
                /* Comprehend word as block number */

                final PreparedStatement prpstmt = connection.prepareStatement("SELECT number FROM blocks WHERE number = ?");
                prpstmt.setString(1, number.toString());

                final ResultSet resultSet = prpstmt.executeQuery();

                if (resultSet.next()) {
                    final JSONArray jsonArrayNumber = new JSONArray();
                    jsonArrayNumber.put("block_number"); // Block exists
                    jsonArrayNumber.put(resultSet.getString(1));
                    jsonArrayResult.put(jsonArrayNumber);
                }
                prpstmt.close();
            } catch (NumberFormatException e) {
            }


            if (searchWord.length() == 64 + 2) {
                /* Search for block */

                final PreparedStatement prpstmt1 = connection.prepareStatement("SELECT NEKH(hash) FROM blocks WHERE hash = UNHEX(?) LIMIT 1");
                prpstmt1.setString(1, searchWord.substring(2));

                final ResultSet resultSet1 = prpstmt1.executeQuery();
                if (resultSet1.next()) {
                    final JSONArray jsonArrayBlock = new JSONArray();
                    jsonArrayBlock.put("block_hash");
                    jsonArrayBlock.put(resultSet1.getString(1));
                    jsonArrayResult.put(jsonArrayBlock);
                }

                prpstmt1.close();

                /* Might be a transaction */

                final PreparedStatement prpstmt3 = connection.prepareStatement("SELECT NEKH(hash) FROM transactions WHERE hash = UNHEX(?) LIMIT 1");
                prpstmt3.setString(1, searchWord.substring(2));

                final ResultSet resultSet3 = prpstmt3.executeQuery();

                if (resultSet3.next()) {
                    final JSONArray jsonArrayTx = new JSONArray();
                    jsonArrayTx.put("transaction_hash");
                    jsonArrayTx.put(resultSet3.getString(1));
                    jsonArrayResult.put(jsonArrayTx);
                }

                prpstmt3.close();
            } else if (searchWord.length() == 40 + 2) {
                /* Might be an address */

                final String hexWithoutPrefix = searchWord.substring(2);

                /* Search for normal address first */
                final PreparedStatement prpstmt1 = connection.prepareStatement("SELECT NEKH(CASE WHEN `to` = UNHEX(?) THEN `to` ELSE `from` END) FROM transactions WHERE `to` = UNHEX(?) OR `from` = UNHEX(?) LIMIT 1");
                prpstmt1.setString(1, hexWithoutPrefix);
                prpstmt1.setString(2, hexWithoutPrefix);
                prpstmt1.setString(3, hexWithoutPrefix);

                final ResultSet resultSet1 = prpstmt1.executeQuery();

                if (resultSet1.next()) {
                    // Found it
                    final JSONArray jsonArrayAddr = new JSONArray();
                    jsonArrayAddr.put("normal_address");
                    jsonArrayAddr.put(resultSet1.getString(1));
                    jsonArrayResult.put(jsonArrayAddr);
                }

                prpstmt1.close();

                /* Search for a contract address, a contract address is created when a contract creation transaction had been executed */

                final PreparedStatement prpstmt2 = connection.prepareStatement("SELECT NEKH(contract_address) FROM transactions WHERE contract_address = UNHEX(?) LIMIT 1");
                prpstmt2.setString(1, hexWithoutPrefix);

                final ResultSet resultSet2 = prpstmt2.executeQuery();

                if (resultSet2.next()) {
                    final JSONArray jsonArrayAddr = new JSONArray();
                    jsonArrayAddr.put("contract_address");
                    jsonArrayAddr.put(resultSet2.getString(1));
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
}
