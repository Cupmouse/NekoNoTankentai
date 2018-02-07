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

public class TransactionRequestHandler implements RequestHandler<TransactionRequestHandler.TransactionRequest> {

    public static final String TRANSACTION_NONCONDITION = "SELECT transactions.internal_id, transactions.block_id, blocks.number, transactions.`index`, NEKH(transactions.hash), " +
            "NEKH(A1.address), NEKH(A2.address), NEKH(A3.address), transactions.`value`, transactions.gas_provided, " +
            "transactions.gas_used, transactions.gas_price, transactions.nonce, NEKH(transactions.input) " +
            "FROM transactions " +
            "LEFT JOIN blocks ON blocks.internal_id = transactions.block_id " +
            "LEFT JOIN addresses AS A1 ON A1.internal_id = transactions.from_id " +
            "LEFT JOIN addresses AS A2 ON A2.internal_id = transactions.to_id " +
            "LEFT JOIN addresses AS A3 ON A3.internal_id = transactions.contract_id " +
            "WHERE ";

    @Override
    public TransactionRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        checkContentIsArray(jsonObject);

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        checkHasParameter(jsonArrayContent);
        final String typeStr = getString(jsonArrayContent, 0, "type");

        if (typeStr.equals("number_and_index")) {

            checkParamCount(jsonArrayContent, 3);

            final BigInteger number = getNonNegativeBigInteger(jsonArrayContent, 1, "number");

            final int index = parseUnsignedInt(jsonArrayContent.get(2), "index");

            return new TransactionRequest.NumberAndIndex(number, index);
        } else if (typeStr.equals("id_and_index")) {

            checkParamCount(jsonArrayContent, 3);

            checkHasString(jsonArrayContent, 1, "internal_id");
            final BigInteger internalId = parseNonNegativeBigInteger(jsonArrayContent.getString(1), "internal_id");

            final int index = parseUnsignedInt(jsonArrayContent.get(2), "index");

            return new TransactionRequest.InternalIdAndIndex(internalId, index);
        } else if (typeStr.equals("hash")) {

            checkParamCount(jsonArrayContent, 2);
            checkHasString(jsonArrayContent, 1, "hash");

            final String hash = jsonArrayContent.getString(1);

            if (!FormatValidateUtil.isValidTransactionHash(hash)) {
                throw new InvalidRequestException("Invalid transaction hash");
            }

            return new TransactionRequest.Hash(hash.substring(2));
        } else {
            throw new InvalidRequestException("Unknown type");
        }
    }



    @Override
    public Object handle(TransactionRequest parameters) throws Exception {
        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            final PreparedStatement prpstmt;

            if (parameters instanceof TransactionRequest.NumberAndIndex) {
                // Number and index

                prpstmt = connection.prepareStatement(TRANSACTION_NONCONDITION + "blocks.forked = 0 AND blocks.number = ? AND transactions.index = ? LIMIT 1");

                final TransactionRequest.NumberAndIndex casted = (TransactionRequest.NumberAndIndex) parameters;

                prpstmt.setString(1, casted.number.toString());
                prpstmt.setInt(2, casted.index);
            } else if (parameters instanceof TransactionRequest.InternalIdAndIndex) {
                // Internal id and index, forked block may return

                prpstmt = connection.prepareStatement(TRANSACTION_NONCONDITION + "transactions.block_id = ? LIMIT 1");

                final TransactionRequest.InternalIdAndIndex casted = (TransactionRequest.InternalIdAndIndex) parameters;

                prpstmt.setString(1, casted.internalId.toString());
                prpstmt.setLong(2, casted.index);

            } else if (parameters instanceof TransactionRequest.Hash) {
                // Hash

                prpstmt = connection.prepareStatement(TRANSACTION_NONCONDITION + "blocks.forked = 0 AND transactions.hash = UNHEX(?) LIMIT 1");
                prpstmt.setString(1, ((TransactionRequest.Hash) parameters).hash);

            } else {
                throw new InvalidRequestException("Key type unknown");
            }

            ResultSet resultSet = prpstmt.executeQuery();   // Execute query

            if (resultSet.next()) {
                JSONObject jsonObjectTx = new JSONObject();
                writeTx(jsonObjectTx,
                        resultSet.getLong(1),
                        resultSet.getLong(2),
                        resultSet.getLong(3),
                        resultSet.getInt(4),
                        resultSet.getString(5),
                        resultSet.getString(6),
                        resultSet.getString(7),
                        resultSet.getString(8),
                        new BigInteger(resultSet.getBytes(9)),
                        resultSet.getLong(10),
                        resultSet.getLong(11),
                        new BigInteger(resultSet.getBytes(12)),
                        resultSet.getString(13),
                        resultSet.getString(14)
                );

                return jsonObjectTx;    // Return result
            } else {
                return false;   // If a transaction was not found, false returns
            }
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred closing a database connection", e);
                }
            }
        }
    }

    private void writeTx(final JSONObject jsonObject,
                         final long internalId, final long blockId, final long blockNumber, final int index, final String hash,
                         final String from, final String to, final String contract, final BigInteger value,
                         final long gasProvided, final long gasUsed, final BigInteger gasPrice, final String nonce, final String input) {
        // This method is separated because I can assure every parameters are the type it is supposed to be
        jsonObject.put("internal_id",   internalId);
        jsonObject.put("block_id",      blockId);
        jsonObject.put("block_number",  blockNumber);
        jsonObject.put("index",         index);
        jsonObject.put("hash",          hash);
        jsonObject.put("from",          from);

        TransactionType txType;

        if (to == null) { // to == null means contract creation transaction
            txType = TransactionType.CONTRACT_CREATION;

            jsonObject.put("contract_address", contract);
        } else {
            if (input.equals("0x")) {
                // Input is empty, this should be normal nuko sending tx
                txType = TransactionType.SEND;
            } else {
                // Assume contract calling
                txType = TransactionType.CONTRACT_CALL;
            }
            txType = TransactionType.SEND;

            jsonObject.put("to", to);
            jsonObject.put("value", value); // Contract calls can also have value to send
        }

        jsonObject.put("type",           txType.name().toLowerCase());

        jsonObject.put("gas_provided",  gasProvided);
        jsonObject.put("gas_used",      gasUsed);
        jsonObject.put("gas_price",     gasPrice);
        jsonObject.put("nonce",         nonce);
        jsonObject.put("input",         input);
    }

    static class TransactionRequest {
        static class Hash extends TransactionRequest {
            private final String hash;

            private Hash(String hash) {
                this.hash = hash;
            }
        }

        static class NumberAndIndex extends TransactionRequest {
            private final BigInteger number;
            private final int index;

            private NumberAndIndex(BigInteger number, int index) {
                this.number = number;
                this.index = index;
            }
        }

        static class InternalIdAndIndex extends TransactionRequest {
            private final BigInteger internalId;
            private final int index;

            private InternalIdAndIndex(BigInteger internalId, int index) {
                this.internalId = internalId;
                this.index = index;
            }
        }
    }
}
