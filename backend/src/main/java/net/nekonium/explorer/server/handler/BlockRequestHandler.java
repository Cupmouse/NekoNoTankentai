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

import static net.nekonium.explorer.server.handler.HandlerCommon.*;

public class BlockRequestHandler implements RequestHandler<BlockRequestHandler.BlockRequest> {

    private static final String BLOCK_NONCONDITION = "SELECT internal_id, number, NEKH(hash), NEKH((SELECT hash FROM blocks AS t WHERE t.internal_id = blocks.parent)), " +
            "UNIX_TIMESTAMP(timestamp), NEKH((SELECT address FROM addresses WHERE addresses.internal_id = blocks.miner_id)), " +
            "difficulty, gas_limit, gas_used, NEKH(extra_data), nonce, size, forked FROM blocks WHERE ";

    @Override
    public BlockRequest parseParameters(final JSONObject jsonObject) throws InvalidRequestException {
        checkContentIsArray(jsonObject);

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        checkParamCount(jsonArrayContent, 2);
        checkHasString(jsonArrayContent, 0, "type");
        checkHasString(jsonArrayContent, 1, "key");

        /* The request's array first element is type which defines the type of the key they sent */

        final String typeStr = jsonArrayContent.getString(0);

        if (typeStr.equals("hash")) {

            final String hash = getBlockHash(jsonArrayContent, 1, "hash");

            return new BlockRequest.Hash(hash.substring(2));          // Block hash
        } else if (typeStr.equals("number")) {

            /* Converts to BigInteger to check if sent string is actually a "number" */

            final BigInteger blockNumber = parseNonNegativeBigInteger(jsonArrayContent.getString(1), "block number");

            return new BlockRequest.Number(blockNumber);   // Block number
        } else if (typeStr.equals("internal_id")) {

            final BigInteger internalId = parseNonNegativeBigInteger(jsonArrayContent.getString(1), "internal id");

            return new BlockRequest.InternalId(internalId);
        } else {
            throw new InvalidRequestException("Unknown key type");
        }
    }

    @Override
    public Object handle(final BlockRequest parameters) throws Exception {
        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            /* Get block data first */

            final PreparedStatement prpstmt;

            if (parameters instanceof BlockRequest.Number) {
                // Returns block on main chain

                prpstmt = connection.prepareStatement(BLOCK_NONCONDITION + "number = ? AND forked = 0 LIMIT 1");
                prpstmt.setString(1, ((BlockRequest.Number) parameters).number.toString());

            } else if (parameters instanceof BlockRequest.InternalId) {
                // Returns whichever block on main or forked

                prpstmt = connection.prepareStatement(BLOCK_NONCONDITION + "internal_id = ? LIMIT 1");
                prpstmt.setString(1, ((BlockRequest.InternalId) parameters).internalId.toString());

            } else if (parameters instanceof BlockRequest.Hash) {
                // type is "hash"
                prpstmt = connection.prepareStatement(BLOCK_NONCONDITION + "hash = UNHEX(?) AND forked = 0 LIMIT 1");
                prpstmt.setString(1, ((BlockRequest.Hash) parameters).hash);

            } else {
                throw new InvalidRequestException("Unknown parameter type");
            }

            final ResultSet resultSet = prpstmt.executeQuery();

            if (!resultSet.next()) {
                prpstmt.close();
                return false;   // If no blocks are found, content will be just boolean false
            }

            /* Parse result set to json */

            final JSONObject jsonObjectContents = new JSONObject();

            final long blockInternalId = resultSet.getLong(1);

            writeBlock(jsonObjectContents,
                    blockInternalId,
                    resultSet.getLong(2),
                    resultSet.getString(3),
                    resultSet.getString(4),
                    resultSet.getLong(5),
                    resultSet.getString(6),
                    resultSet.getString(7),
                    resultSet.getLong(8),
                    resultSet.getLong(9),
                    resultSet.getString(10),
                    resultSet.getString(11),
                    resultSet.getInt(12),
                    resultSet.getBoolean(13)
            );

            prpstmt.close();

            /* Get uncle blocks */

            final Object uncleBlocks = getUncleBlocks(connection, blockInternalId);
            jsonObjectContents.put("uncles", uncleBlocks);

            return jsonObjectContents;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred when closing an database connection", e);
                }
            }
        }
    }

    // TODO Change type here if extra space for number or internal_id is needed

    private void writeBlock(JSONObject jsonObject,
                                  long internalId, long number, String hash, String parentHash, long timestamp, String miner,
                                  String difficulty, long gasLimit, long gasUsed, String extraData, String nonce,
                                  int size, boolean forked) {
        jsonObject.put("internal_id",   internalId);
        jsonObject.put("number",        number);
        jsonObject.put("hash",          hash);
        jsonObject.put("parentHash",    parentHash);
        jsonObject.put("timestamp",     timestamp);
        jsonObject.put("miner",         miner);
        jsonObject.put("difficulty",    difficulty);
        jsonObject.put("gas_limit",     gasLimit);
        jsonObject.put("gas_used",      gasUsed);
        jsonObject.put("extra_data",    extraData);
        jsonObject.put("nonce",         nonce);
        jsonObject.put("size",          size);
        jsonObject.put("forked",        forked);
    }

    private JSONArray getUncleBlocks(Connection connection, long blockInternalId) throws SQLException, InvalidRequestException {
        final JSONArray jsonArrayUncles = new JSONArray();

        final PreparedStatement prpstmt = connection.prepareStatement("SELECT NEKH(hash), number FROM uncle_blocks WHERE block_id = ?");
        prpstmt.setLong(1, blockInternalId);
        final ResultSet resultSet = prpstmt.executeQuery();

        while (resultSet.next()) {
            final JSONArray jsonArrayUncle = new JSONArray();

            jsonArrayUncle.put(resultSet.getString(1)); // Hash
            jsonArrayUncle.put(resultSet.getString(2)); // Number

            jsonArrayUncles.put(jsonArrayUncle);
        }

        prpstmt.close();

        return jsonArrayUncles;
    }

    static class BlockRequest {

        static class Hash extends BlockRequest {
            private final String hash;

            private Hash(String hash) {
                this.hash = hash;
            }
        }

        static class Number extends BlockRequest {
            private final BigInteger number;

            private Number(BigInteger number) {
                this.number = number;
            }
        }

        static class InternalId extends BlockRequest {
            private final BigInteger internalId;

            private InternalId(BigInteger internalId) {
                this.internalId = internalId;
            }
        }
    }
}
