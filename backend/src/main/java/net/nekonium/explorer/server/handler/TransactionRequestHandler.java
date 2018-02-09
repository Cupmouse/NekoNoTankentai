package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.server.handler.TransactionRequestHandler.TransactionRequest.Hash;
import net.nekonium.explorer.server.handler.TransactionRequestHandler.TransactionRequest.HashAndIndex;
import net.nekonium.explorer.server.handler.TransactionRequestHandler.TransactionRequest.InternalIdAndIndex;
import net.nekonium.explorer.server.handler.TransactionRequestHandler.TransactionRequest.NumberAndIndex;
import net.nekonium.explorer.util.FormatValidateUtil;
import net.nekonium.explorer.util.NonNullPair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static net.nekonium.explorer.server.handler.HandlerCommon.*;

public class TransactionRequestHandler implements RequestHandler<TransactionRequestHandler.TransactionRequest> {

    private static final String TRANSACTION_NONCONDITION =
            "SELECT transactions.internal_id, transactions.block_id, blocks.number, transactions.`index`, NEKH(transactions.hash), " +
            "NEKH(A1.address), NEKH(A2.address), NEKH(A3.address), transactions.`value`, transactions.gas_provided, " +
            "transactions.gas_used, transactions.gas_price, transactions.nonce, NEKH(transactions.input), transactions.input = 0, blocks.forked " +
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

        if (typeStr.equals("hash-and-index")) {

            checkParamCount(jsonArrayContent, 3);

            final String blockHash = getString(jsonArrayContent, 1, "blockHash");

            if (!FormatValidateUtil.isValidBlockHash(blockHash)) {
                throw new InvalidRequestException("Invalid block hash");
            }

            final int txIndex = getUnsignedInt(jsonArrayContent, 2, "txIndex");

            return new HashAndIndex(blockHash, txIndex);

        } else if (typeStr.equals("number-and-index")) {

            checkParamCount(jsonArrayContent, 3);

            final BigInteger number = getNonNegativeBigInteger(jsonArrayContent, 1, "number");

            final int index = parseUnsignedInt(jsonArrayContent.get(2), "index");

            return new NumberAndIndex(number, index);
        } else if (typeStr.equals("id-and-index")) {

            checkParamCount(jsonArrayContent, 3);

            checkHasString(jsonArrayContent, 1, "internal_id");
            final BigInteger internalId = parseNonNegativeBigInteger(jsonArrayContent.getString(1), "internal_id");

            final int index = parseUnsignedInt(jsonArrayContent.get(2), "index");

            return new InternalIdAndIndex(internalId, index);
        } else if (typeStr.equals("hash")) {

            checkParamCount(jsonArrayContent, 2);
            checkHasString(jsonArrayContent, 1, "hash");

            final String hash = jsonArrayContent.getString(1);

            if (!FormatValidateUtil.isValidTransactionHash(hash)) {
                throw new InvalidRequestException("Invalid transaction hash");
            }

            return new Hash(hash.substring(2));
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

            if (parameters instanceof HashAndIndex) {

                prpstmt = connection.prepareStatement(TRANSACTION_NONCONDITION + "blocks.hash = UNHEX(?) AND transactions.`index` = ? AND blocks.forked = 0 LIMIT 1");
            } else if (parameters instanceof NumberAndIndex) {
                // Number and index

                prpstmt = connection.prepareStatement(TRANSACTION_NONCONDITION + "blocks.number = ? AND transactions.index = ? AND blocks.forked = 0 LIMIT 1");

                final NumberAndIndex casted = (NumberAndIndex) parameters;

                prpstmt.setString(1, casted.number.toString());
                prpstmt.setInt(2, casted.index);
            } else if (parameters instanceof InternalIdAndIndex) {
                // Internal id and index, forked block may return

                prpstmt = connection.prepareStatement(TRANSACTION_NONCONDITION + "transactions.block_id = ? AND transactions.`index` = ? LIMIT 1");

                final InternalIdAndIndex casted = (InternalIdAndIndex) parameters;

                prpstmt.setString(1, casted.internalId.toString());
                prpstmt.setLong(2, casted.index);

            } else if (parameters instanceof Hash) {
                // Hash

                prpstmt = connection.prepareStatement(TRANSACTION_NONCONDITION + "transactions.hash = UNHEX(?) AND blocks.forked = 0 LIMIT 1");
                prpstmt.setString(1, ((Hash) parameters).hash);

            } else {
                throw new InvalidRequestException("Key type unknown");
            }

            ResultSet resultSet = prpstmt.executeQuery();   // Execute query

            if (resultSet.next()) {
                JSONArray jsonArrayTx = new JSONArray();
                writeTx(jsonArrayTx,
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
                        resultSet.getString(14),
                        resultSet.getBoolean(15)
                );

                return jsonArrayTx;    // Return result
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

    private void writeTx(final JSONArray jsonArray,
                         final long internalId, final long blockId, final long blockNumber, final int index, final String hash,
                         final String from, final String to, final String contract, final BigInteger value,
                         final long gasProvided, final long gasUsed, final BigInteger gasPrice, final String nonce, final String input, boolean emptyInput) throws InvalidRequestException {
        // This method is separated because I can assure every parameters are the type it is supposed to be

        final NonNullPair<TransactionType, String> pair = HandlerCommon.determineTxTypeAndTargetAddress(to, contract, emptyInput);

        jsonArray.put(pair.getA().toString());
        jsonArray.put(internalId);
        jsonArray.put(blockId);
        jsonArray.put(blockNumber);
        jsonArray.put(index);
        jsonArray.put(hash);
        jsonArray.put(from);
        jsonArray.put(pair.getB());
        jsonArray.put(gasProvided);
        jsonArray.put(gasUsed);
        jsonArray.put(gasPrice);
        jsonArray.put(nonce);
        jsonArray.put(input);
    }

    static class TransactionRequest {
        static class Hash extends TransactionRequest {
            private final String hash;

            private Hash(String hash) {
                this.hash = hash;
            }
        }

        static class HashAndIndex extends TransactionRequest {
            private final String blockHash;
            private final int txIndex;

            private HashAndIndex(String blockHash, int txIndex) {
                this.blockHash = blockHash;
                this.txIndex = txIndex;
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
