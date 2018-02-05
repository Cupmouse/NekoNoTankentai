package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.AddressType;
import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.util.IllegalDatabaseStateException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static net.nekonium.explorer.server.handler.HandlerCommon.*;

public class AddressRequestHandler implements RequestHandler<AddressRequestHandler.AddressRequest> {

    @Override
    public AddressRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        checkContentIsArray(jsonObject);

        JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        checkHasParameter(jsonArrayContent);

        String typeStr = getString(jsonArrayContent, 0, "type");

        if (typeStr.equals("hash")) {
            checkParamCount(jsonArrayContent, 2);

            String hash = getString(jsonArrayContent, 1, "address_hash");

            return new AddressRequestHandler.AddressRequest.Hash(hash.substring(2));
        } else {
            throw new InvalidRequestException("Unknown type");
        }
    }

    @Override
    public Object handle(AddressRequest parameters) throws Exception {
        Connection connection = null;
        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            if (parameters instanceof AddressRequest.Hash) {

                // Gather common information
                final PreparedStatement prpstmt1 = connection.prepareStatement(
                        "SELECT internal_id, NEKH(address), type, alias, description, " +
                                "(SELECT COUNT(*) FROM transactions WHERE from_id = addresses.internal_id)" +
                                " + (SELECT COUNT(*) FROM transactions WHERE to_id = addresses.internal_id), " +
                                "(SELECT COUNT(*) FROM blocks WHERE blocks.miner_id = addresses.internal_id)" +
                                " + (SELECT COUNT(*) FROM uncle_blocks WHERE uncle_blocks.miner_id = addresses.internal_id) " +
                                "FROM addresses WHERE address = UNHEX(?)");
                prpstmt1.setString(1, ((AddressRequest.Hash) parameters).hash);

                final ResultSet resultSet1 = prpstmt1.executeQuery();

                if (!resultSet1.next()) {    // Return false if there is no hit
                    return false;
                }

                final JSONArray jsonArray = new JSONArray();

                final long internalId = resultSet1.getLong(1);

                // Put all info
                jsonArray.put(internalId);
                jsonArray.put(resultSet1.getString(2)); // Hash
                jsonArray.put(AddressType.valueOf(resultSet1.getString(3)));    // Address type
                jsonArray.put(resultSet1.getString(4)); // Alias
                jsonArray.put(resultSet1.getString(5)); // Description
                jsonArray.put(resultSet1.getLong(6));   // Transaction count
                jsonArray.put(resultSet1.getLong(7));   // Mined block count

                resultSet1.close();
                prpstmt1.close();

                // Get latest and first appearance on blockchain

                // Note: It is sorted by tx table's internal_id for fast search (using blocks.number will slow down query a lot)
                // So it may returns an incorrect result because transaction's internal_id is not always in the order as block's number do
                final PreparedStatement prpstmt2 = connection.prepareStatement(
                        "(SELECT blocks.number, UNIX_TIMESTAMP(blocks.timestamp) FROM balance " +
                                "LEFT JOIN blocks ON blocks.internal_id = balance.block_id " +
                                "WHERE balance.address_id = ? ORDER BY balance.number ASC LIMIT 1)" +
                                "UNION ALL " +
                                "(SELECT blocks.number, UNIX_TIMESTAMP(blocks.timestamp) FROM transactions " +
                                "LEFT JOIN blocks ON blocks.internal_id = transactions.block_id " +
                                "WHERE (from_id = ? OR to_id = ?) AND blocks.forked = 0 " +
                                "ORDER BY transactions.internal_id DESC LIMIT 1)");
                prpstmt2.setLong(1, internalId);
                prpstmt2.setLong(2, internalId);
                prpstmt2.setLong(3, internalId);

                final ResultSet resultSet2 = prpstmt2.executeQuery();

                if (resultSet2.next()) {
                    // There are matching tx, there should be 2 rows

                    // First appearance
                    jsonArray.put(resultSet2.getLong(1));       // Block number
                    jsonArray.put(resultSet2.getLong(2));     // Block timestamp

                    resultSet2.next();  // Move to the next row

                    // Last appearance
                    jsonArray.put(resultSet2.getLong(1));
                    jsonArray.put(resultSet2.getLong(2));
                } else {
                    // Weird, there should be any result if the address is recorded
                    throw new IllegalDatabaseStateException("Address appearance data did not found on the database");
                }

                resultSet2.close();
                prpstmt2.close();

                // TODO Tx count and mining count ?

                return jsonArray;
            } else {
                throw new InvalidRequestException("Unknown type");
            }

        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred when closing a connection", e);
                }
            }
        }
    }

    static class AddressRequest {

        static class Hash extends AddressRequest {

            private String hash;

            private Hash(String hash) {
                this.hash = hash;
            }
        }
    }
}
