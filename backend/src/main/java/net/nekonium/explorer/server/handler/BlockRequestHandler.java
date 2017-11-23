package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.util.FormatValidator;
import net.nekonium.explorer.util.TypeConversion;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Optional;

public class BlockRequestHandler implements RequestHandler<BlockRequestHandler.BlockRequest> {

    @Override
    public BlockRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        if (!(jsonObject.get("content") instanceof JSONArray)) {
            throw new InvalidRequestException("Lack of proper content node");
        }

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        if (jsonArrayContent.length() != 2) {
            // Need type and key parameters
            throw new InvalidRequestException("Missing or too much array elements (2 expected)");
        }
        if (!(jsonArrayContent.get(0) instanceof String) || !(jsonArrayContent.get(1) instanceof String)) {
            // type and key supposed to be string object
            throw new InvalidRequestException("'type' and 'key' have to be string");
        }

        final String typeStr = jsonArrayContent.getString(0).toUpperCase();

        final BlockRequest.Type type = BlockRequest.Type.valueOf(typeStr);

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

            return new BlockRequest(blockNumber);   // Block number
        } else {
            /* type == HASH */

            final String hash = jsonArrayContent.getString(1);

            if (!FormatValidator.isValidTransactionHash(hash)) {    // Check if hash is valid
                throw new InvalidRequestException("Block hash is invalid");
            }

            return new BlockRequest(hash);          // Block hash
        }
    }

    @Override
    public Object handle(BlockRequest parameters) throws Exception {
        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            /* Get block data first */

            final PreparedStatement prpstmt1;

            if (parameters.type == BlockRequest.Type.NUMBER) {

                // TODO Forked blocks ?
                prpstmt1 = connection.prepareStatement(
                        "SELECT internal_id, number, NEKH(hash), NEKH(parent_hash), UNIX_TIMESTAMP(timestamp), " +
                                "NEKH(miner), difficulty, gas_limit, gas_used, NEKH(extra_data), nonce, NEKH(sha3_uncles), size " +
                                "FROM blocks WHERE number = ? LIMIT 1");
                prpstmt1.setString(1, parameters.key.toString());
            } else {
                // type is "hash"
                prpstmt1 = connection.prepareStatement("SELECT * FROM blocks WHERE hash = UNHEX(?)");
                prpstmt1.setString(1, ((String) parameters.key));
            }

            final ResultSet resultSet = prpstmt1.executeQuery();

            /* Parse result set to json */

            final JSONObject jsonObjectContents = new JSONObject();

            final BigInteger internal_id;

            if (resultSet.next()) {
                int n = 0;  // Count column number

                internal_id = new BigInteger(resultSet.getString(++n));
                jsonObjectContents.put("internal_id"    , internal_id);
                jsonObjectContents.put("number"         , new BigInteger(resultSet.getString(++n)));
                jsonObjectContents.put("hash"           , resultSet.getString(++n));
                jsonObjectContents.put("parent_hash"    , resultSet.getString(++n));
                jsonObjectContents.put("timestamp"      , resultSet.getInt(++n));
                jsonObjectContents.put("miner"          , resultSet.getString(++n));
                jsonObjectContents.put("difficulty"     , new BigInteger(resultSet.getString(++n)));
                jsonObjectContents.put("gasLimit"       , resultSet.getLong(++n));
                jsonObjectContents.put("gasUsed"        , resultSet.getLong(++n));
                jsonObjectContents.put("extra_data"     , resultSet.getString(++n));
                jsonObjectContents.put("nonce"          , new BigInteger(resultSet.getString(++n)));
                jsonObjectContents.put("sha3_uncles"    , resultSet.getString(++n));
                jsonObjectContents.put("size"           , resultSet.getInt(++n));
            } else {
                return false;   // If no blocks found, content will be just boolean false
            }

            prpstmt1.close();

            /* Get transactions */

            final PreparedStatement prpstmt2 = connection.prepareStatement(
                    "SELECT internal_id, `index`, NEKH(hash), NEKH(`from`), NEKH(`to`), NEKH(contract_address), value, gas_provided, " +
                            "gas_used, gas_price, nonce, NEKH(input) FROM transactions WHERE block_id = ? ORDER BY `index` ASC");
            prpstmt2.setString(1, internal_id.toString());

            final ResultSet resultSet2 = prpstmt2.executeQuery();

            final JSONArray jsonArrayTransactions = new JSONArray();

            while (resultSet2.next()) {
                final JSONObject jsonObjectTransaction = new JSONObject();

                TransactionType transactionType = null;
                if (resultSet2.getString(6) != null) {
                    transactionType = TransactionType.CONTRACT_CREATION;
                } else {
                    transactionType = TransactionType.SENDING;
                }

                int n = 0;

                jsonObjectTransaction.put("type",           transactionType.name().toLowerCase());
                jsonObjectTransaction.put("internal_id",    new BigInteger(resultSet2.getString(++n)));
                jsonObjectTransaction.put("index",          resultSet2.getInt(++n));
                jsonObjectTransaction.put("hash",           resultSet2.getString(++n));
                jsonObjectTransaction.put("from",           resultSet2.getString(++n));

                if (transactionType == TransactionType.SENDING) {
                    jsonObjectTransaction.put("to",             resultSet2.getString(++n));
                    n++;
                    jsonObjectTransaction.put("value",          new BigInteger(resultSet2.getBytes(++n)));
                } else {
                    n += 2;
                    jsonObjectTransaction.put("contract_address", resultSet2.getString(++n));
                }

                jsonObjectTransaction.put("gas_provided",   resultSet2.getInt(++n));
                jsonObjectTransaction.put("gas_used",       resultSet2.getInt(++n));
                jsonObjectTransaction.put("gas_price",      new BigInteger(resultSet2.getBytes(++n)));
                jsonObjectTransaction.put("nonce",          new BigInteger(resultSet2.getString(++n)));
                jsonObjectTransaction.put("input",          resultSet2.getString(++n));

                jsonArrayTransactions.put(jsonObjectTransaction);
            }

            jsonObjectContents.put("transactions", jsonArrayTransactions);

            prpstmt2.close();

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

    enum TransactionType {
        SENDING, CONTRACT_CREATION
    }

    static class BlockRequest {

        private final Type type;
        private final Object key;

        private BlockRequest(String hash) {
            this.type = Type.HASH;
            this.key = hash;
        }

        private BlockRequest(BigInteger number) {
            this.type = Type.NUMBER;
            this.key = number;
        }

        enum Type {
            NUMBER, HASH
        }
    }
}
