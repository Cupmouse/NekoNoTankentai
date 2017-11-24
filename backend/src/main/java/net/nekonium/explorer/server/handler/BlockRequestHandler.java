package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.util.FormatValidator;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static net.nekonium.explorer.server.handler.HandlerCommon.BLOCK_COLUMN;
import static net.nekonium.explorer.util.JSONUtil.hasString;

public class BlockRequestHandler implements RequestHandler<BlockRequestHandler.BlockRequest> {

    @Override
    public BlockRequest parseParameters(final JSONObject jsonObject) throws InvalidRequestException {
        if (!(jsonObject.get("content") instanceof JSONArray)) {
            throw new InvalidRequestException("Lack of proper content node");
        }

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        if (jsonArrayContent.length() != 4) {
            // Need type and key parameters
            throw new InvalidRequestException("Missing or too much array elements (4 expected)");
        }
        if (!hasString(jsonArrayContent, 0) || !hasString(jsonArrayContent, 1)) {
            // type and key supposed to be string object
            throw new InvalidRequestException("'type' and 'key' have to be string");
        }
        if (!hasString(jsonArrayContent, 2) || !hasString(jsonArrayContent, 3)) {
            throw new InvalidRequestException("'transaction_detail' and 'unclde_detail' have to be string");
        }

        /* The request's array first element is type which defines the type of the key they sent */

        final String typeStr = jsonArrayContent.getString(0).toUpperCase();
        final BlockRequest.Type type;

        try {
            type = BlockRequest.Type.valueOf(typeStr);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Key type unknown", e);
        }

        /* Before parsing the key, get transaction detail and uncle detail */
        /* Is detailed transaction data needed? or just trasaction hashes, transaction count or no data */

        final BlockRequest.TransactionDetail transactionDetail;

        try {
            transactionDetail = BlockRequest.TransactionDetail.valueOf(jsonArrayContent.getString(2).toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Transaction detail unknown", e);
        }

        final BlockRequest.UncleDetail uncleDetail;

        try {
            uncleDetail = BlockRequest.UncleDetail.valueOf(jsonArrayContent.getString(3).toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Uncle detail unknown", e);
        }

        /* Now let's parse key */

        if (type == BlockRequest.Type.NUMBER) {
            final String blockNumStr = jsonArrayContent.getString(1);

            /* Converts to BigInteger to check if sent string is actually a "number" */

            final BigInteger blockNumber;

            try {
                blockNumber = new BigInteger(blockNumStr);
            } catch (NumberFormatException e) {
                throw new InvalidRequestException("Block number has to be numeric", e);
            }

            if (blockNumber.compareTo(BigInteger.ZERO) < 0) {
                throw new InvalidRequestException("Block number cannot be negative");
            }

            return new BlockRequest(blockNumber, transactionDetail, uncleDetail);   // Block number
        } else {
            /* type == HASH */

            final String hash = jsonArrayContent.getString(1);

            if (!FormatValidator.isValidBlockHash(hash)) {    // Check if hash is valid
                throw new InvalidRequestException("Block hash is invalid");
            }

            return new BlockRequest(hash.substring(2), transactionDetail, uncleDetail);          // Block hash
        }
    }

    @Override
    public Object handle(final BlockRequest parameters) throws Exception {
        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            /* Get block data first */

            final PreparedStatement prpstmt;

            if (parameters.type == BlockRequest.Type.NUMBER) {

                // TODO Forked blocks ?
                prpstmt = connection.prepareStatement(
                        "SELECT " + BLOCK_COLUMN + " FROM blocks WHERE number = ? LIMIT 1");
                prpstmt.setString(1, parameters.key.toString());
            } else {
                // type is "hash"
                prpstmt = connection.prepareStatement("SELECT " + BLOCK_COLUMN + " FROM blocks WHERE hash = UNHEX(?)");
                prpstmt.setString(1, ((String) parameters.key));
            }

            final ResultSet resultSet = prpstmt.executeQuery();

            /* Parse result set to json */

            final JSONObject jsonObjectContents = new JSONObject();

            final BigInteger blockInternalId;

            if (resultSet.next()) {
                int n = 0;  // Count column number

                blockInternalId = new BigInteger(resultSet.getString(++n));
                jsonObjectContents.put("internal_id"    , blockInternalId);
                jsonObjectContents.put("number"         , resultSet.getString(++n));
                jsonObjectContents.put("hash"           , resultSet.getString(++n));
                jsonObjectContents.put("parent_hash"    , resultSet.getString(++n));
                jsonObjectContents.put("timestamp"      , resultSet.getLong(++n));
                jsonObjectContents.put("miner"          , resultSet.getString(++n));
                jsonObjectContents.put("difficulty"     , resultSet.getString(++n));
                jsonObjectContents.put("gas_limit"      , resultSet.getLong(++n));
                jsonObjectContents.put("gas_used"       , resultSet.getLong(++n));
                jsonObjectContents.put("extra_data"     , resultSet.getString(++n));
                jsonObjectContents.put("nonce"          , resultSet.getString(++n));
                jsonObjectContents.put("sha3_uncles"    , resultSet.getString(++n));
                jsonObjectContents.put("size"           , resultSet.getInt(++n));
            } else {
                return false;   // If no blocks found, content will be just boolean false
            }

            prpstmt.close();

            if (parameters.transactionDetail != BlockRequest.TransactionDetail.NOT_INCLUDE) {
                /* Get transactions */

                Object transactions = getTransactions(connection, blockInternalId, parameters.transactionDetail);
                jsonObjectContents.put("transactions", transactions);
            }

            if (parameters.uncleDetail != BlockRequest.UncleDetail.NOT_INCLUDE) {
                /* Get uncle blocks */

                final Object uncleBlocks = getUncleBlocks(connection, blockInternalId, parameters.uncleDetail);
                jsonObjectContents.put("uncles", uncleBlocks);
            }


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

    private Object getUncleBlocks(Connection connection, BigInteger blockInternalId, BlockRequest.UncleDetail detail) throws SQLException {
        if (detail == BlockRequest.UncleDetail.COUNT) {
            /* Returns uncle count of the block */
            final PreparedStatement prpstmt = connection.prepareStatement("SELECT 1 FROM uncle_blocks WHERE block_id = ? ORDER BY `index` ASC");
            prpstmt.setString(1, blockInternalId.toString());

            final ResultSet resultSet = prpstmt.executeQuery();
            resultSet.last();
            final int count = resultSet.getRow();

            prpstmt.close();

            return count;
        } else {
            final JSONArray jsonArrayUncles = new JSONArray();

            if (detail == BlockRequest.UncleDetail.ALL) {
                final PreparedStatement prpstmt = connection.prepareStatement(
                        "SELECT " + HandlerCommon.UNCLE_COLUMNS + " FROM uncle_blocks WHERE block_id = ? ORDER BY `index` ASC");
                prpstmt.setString(1, blockInternalId.toString());

                final ResultSet resultSet = prpstmt.executeQuery();

                while (resultSet.next()) {
                    jsonArrayUncles.put(HandlerCommon.getUncle(resultSet));
                }

                prpstmt.close();
            } else if (detail == BlockRequest.UncleDetail.HASH_ONLY) {
                final PreparedStatement prpstmt = connection.prepareStatement("SELECT NEKH(hash) FROM uncle_blocks WHERE block_id = ?");
                final ResultSet resultSet = prpstmt.executeQuery();

                while (resultSet.next()) {
                    jsonArrayUncles.put(resultSet.getString(1));
                }

                prpstmt.close();
            }

            return jsonArrayUncles;
        }
    }

    private Object getTransactions(Connection connection, BigInteger blockInternalId, BlockRequest.TransactionDetail detail) throws SQLException {
        if (detail == BlockRequest.TransactionDetail.COUNT) {
            final PreparedStatement prpstmt = connection.prepareStatement("SELECT 1 FROM transactions WHERE block_id = ?");
            prpstmt.setString(1, blockInternalId.toString());

            final ResultSet resultSet = prpstmt.executeQuery();
            resultSet.last();
            final int count = resultSet.getRow();

            prpstmt.close();

            return count;   // Returns transaction count on the block
        } else {
            /* Here returns JSONArray */
            final JSONArray jsonArrayTransactions = new JSONArray();

            if (detail == BlockRequest.TransactionDetail.ALL) {
                final PreparedStatement prpstmt = connection.prepareStatement(
                        "SELECT " + HandlerCommon.TRANSACTION_COLUMN + " FROM transactions WHERE block_id = ? ORDER BY `index` ASC");
                prpstmt.setString(1, blockInternalId.toString());

                final ResultSet resultSet = prpstmt.executeQuery();

                while (resultSet.next()) {
                    jsonArrayTransactions.put(HandlerCommon.getTransaction(resultSet));
                }

                prpstmt.close();
            } else if (detail == BlockRequest.TransactionDetail.HASH_ONLY) {
                final PreparedStatement prpstmt =
                        connection.prepareStatement("SELECT NEKH(hash) FROM transactions WHERE block_id = ? ORDER BY `index` ASC");
                prpstmt.setString(1, blockInternalId.toString());

                final ResultSet resultSet = prpstmt.executeQuery();

                while (resultSet.next()) {
                    jsonArrayTransactions.put(resultSet.getString(1));
                }

                prpstmt.close();
            }

            return jsonArrayTransactions;
        }
    }

    static class BlockRequest {

        private final Type type;
        private final Object key;
        private final TransactionDetail transactionDetail;
        private final UncleDetail uncleDetail;

        private BlockRequest(String hash, TransactionDetail transactionDetail, UncleDetail uncleDetail) {
            this.type = Type.HASH;
            this.key = hash;
            this.transactionDetail = transactionDetail;
            this.uncleDetail = uncleDetail;
        }

        private BlockRequest(BigInteger number, TransactionDetail transactionDetail, UncleDetail uncleDetail) {
            this.type = Type.NUMBER;
            this.key = number;
            this.transactionDetail = transactionDetail;
            this.uncleDetail = uncleDetail;
        }

        enum Type {
            NUMBER, HASH
        }

        enum TransactionDetail {
            ALL, HASH_ONLY, COUNT, NOT_INCLUDE
        }

        enum UncleDetail {
            ALL, HASH_ONLY, COUNT, NOT_INCLUDE
        }
    }
}
