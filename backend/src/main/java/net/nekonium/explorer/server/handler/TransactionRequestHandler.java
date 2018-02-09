package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.server.handler.TransactionRequestHandler.TransactionRequest.Hash;
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

        if (typeStr.equals("hash")) {

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

            if (parameters instanceof Hash) {
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
        jsonArray.put(value.toString());
        jsonArray.put(gasProvided);
        jsonArray.put(gasUsed);
        jsonArray.put(gasPrice.toString());
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
    }
}
