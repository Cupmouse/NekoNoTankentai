package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.util.FormatValidateUtil;
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

            final String hash = jsonArrayContent.getString(1);

            if (!FormatValidateUtil.isValidBlockHash(hash)) {    // Check if hash is valid
                throw new InvalidRequestException("Block hash is invalid");
            }

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

            /* Get transactions */

            final Object transactions = getTransactions(connection, blockInternalId);
            jsonObjectContents.put("transactions", transactions);

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

    private JSONArray getTransactions(Connection connection, long blockInternalId) throws SQLException, InvalidRequestException {
        final JSONArray jsonArrayTransactions = new JSONArray();    // Initialize json array

        final PreparedStatement prpstmt = connection.prepareStatement(
                "SELECT transactions.internal_id, NEKH(transactions.hash), " +
                        "NEKH(A1.address), NEKH(A2.address), NEKH(A3.address), transactions.`value`, input = 0 " +
                        "FROM transactions " +
                        "LEFT JOIN addresses AS A1 ON A1.internal_id = transactions.from_id " +
                        "LEFT JOIN addresses AS A2 ON A2.internal_id = transactions.to_id " +
                        "LEFT JOIN addresses AS A3 ON A3.internal_id = transactions.contract_id " +
                        "WHERE transactions.block_id = ? ORDER BY `index` ASC");
        // Result is sorted by index in an ascending order

        prpstmt.setLong(1, blockInternalId);

        ResultSet resultSet = prpstmt.executeQuery();

        while (resultSet.next()) {  // Add txs
            JSONObject jsonObjectTx = new JSONObject();

            final String to = resultSet.getString(4);
            final String contract = resultSet.getString(5);
            final boolean emptyInput = resultSet.getBoolean(7);
            final String target;
            final String txType;

            if (to != null) {
                target = to;

                if (emptyInput) {
                    // Consider this tx as a normal sending
                    txType = "s";
                } else {
                    // Contract execution
                    txType = "ce";
                }
            } else if (contract != null) {
                target = contract;
                txType = "cc";
            } else {
                throw new InvalidRequestException("Unknown transaction type");
            }

            jsonObjectTx.put("type", txType);
            jsonObjectTx.put("internal_id", resultSet.getLong(1));
            jsonObjectTx.put("hash", resultSet.getString(2));
            jsonObjectTx.put("from", resultSet.getString(3));
            jsonObjectTx.put("target", target);
            jsonObjectTx.put("value", new BigInteger(resultSet.getBytes(6)));

            jsonArrayTransactions.put(jsonObjectTx);
        }

        prpstmt.close();

        return jsonArrayTransactions;
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
