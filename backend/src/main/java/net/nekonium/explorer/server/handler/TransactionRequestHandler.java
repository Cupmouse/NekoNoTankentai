package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.util.FormatValidator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static net.nekonium.explorer.util.JSONUtil.hasJSONArray;
import static net.nekonium.explorer.util.JSONUtil.hasString;

public class TransactionRequestHandler implements RequestHandler<TransactionRequestHandler.TransactionRequest> {

    @Override
    public TransactionRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        if (!hasJSONArray(jsonObject, "content")) {
            throw new InvalidRequestException("'content' has to be array");
        }

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        if (jsonArrayContent.length() < 1) {
            throw new InvalidRequestException("No parameters found");
        }

        if (!hasString(jsonArrayContent, 0)) {
            throw new InvalidRequestException("'type' has to be string");
        }

        final String typeStr = jsonArrayContent.getString(0).toLowerCase();

        if (typeStr.equals("number_and_index")) {
            if (jsonArrayContent.length() != 3) {
                throw new InvalidRequestException("Too much element or not enough parameters");
            }
            if (!hasString(jsonArrayContent, 1) && !hasString(jsonArrayContent, 2)) {
                throw new InvalidRequestException("'number' and 'index' have to be string");
            }

            final BigInteger number;
            try {
                number = new BigInteger(jsonArrayContent.getString(1));
            } catch (JSONException e) {
                throw new InvalidRequestException("'number' has to be numeric");
            }

            if (!(jsonArrayContent.get(2) instanceof Number)) {
                throw new InvalidRequestException("'index' has to be numeric");
            }

            final int index = jsonArrayContent.getInt(2);

            return new TransactionRequest.NumberAndIndex(number, index);
        } else if (typeStr.equals("hash")) {
            if (jsonArrayContent.length() != 2) {
                throw new InvalidRequestException("Too much element or not enough parameters");
            }
            if (!hasString(jsonArrayContent, 1)) {
                throw new InvalidRequestException("'hash' has to be string");
            }
            if (!FormatValidator.isValidTransactionHash(jsonArrayContent.getString(1))) {
                throw new InvalidRequestException("Invalid transaction hash");
            }

            return new TransactionRequest.Hash(jsonArrayContent.getString(1).substring(2));
        } else {
            throw new InvalidRequestException("Unknown type");
        }
    }

    @Override
    public Object handle(TransactionRequest parameters) throws Exception {
        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            final Object transaction;

            if (parameters instanceof TransactionRequest.NumberAndIndex) {
                // Number and index

                transaction = HandlerCommon.getTransaction(connection, "blocks.forked = 0 AND blocks.number = ? AND transactions.index = ? LIMIT 1", prpstmt -> {
                    prpstmt.setString(1, ((TransactionRequest.NumberAndIndex) parameters).number.toString());
                    prpstmt.setInt(2, ((TransactionRequest.NumberAndIndex) parameters).index);
                }, false);
            } else {
                // Hash

                transaction = HandlerCommon.getTransaction(connection, "blocks.forked = 0 AND transaction.hash = UNHEX(?) LIMIT 1", prpstmt -> {
                    prpstmt.setString(1, ((TransactionRequest.Hash) parameters).hash);
                }, false);
            }

            return transaction; // If transaction was not found, false returns
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
    }
}
