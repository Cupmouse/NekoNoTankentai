package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.util.FormatValidator;
import net.nekonium.explorer.util.JSONUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static net.nekonium.explorer.server.handler.HandlerCommon.UNCLE_COLUMNS;
import static net.nekonium.explorer.server.handler.HandlerCommon.getUncle;

public class UncleRequestHandler implements RequestHandler<UncleRequestHandler.UncleRequest> {

    @Override
    public UncleRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        if (!JSONUtil.hasJSONArray(jsonObject, "content")) {
            throw new InvalidRequestException("'content' has to be array");
        }

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        if (jsonArrayContent.length() < 1) {
            throw new InvalidRequestException("No parameters found");
        }

        if (!JSONUtil.hasString(jsonArrayContent, 0)) {
            throw new InvalidRequestException("'type' has to be string");
        }

        final String typeStr = jsonArrayContent.getString(0);

        if (typeStr.equals("number_and_index")) {
            if (jsonArrayContent.length() != 3) {
                throw new InvalidRequestException("Too much or not enough parameters");
            }

            if (!JSONUtil.hasString(jsonArrayContent, 1)) {
                throw new InvalidRequestException("'number' has to be string");
            }

            if (!JSONUtil.hasNumber(jsonArrayContent, 2)) {
                throw new InvalidRequestException("'index' has to be number");
            }

            final BigInteger number;

            try {
                number = new BigInteger(jsonArrayContent.getString(1));
            } catch (NumberFormatException e) {
                throw new InvalidRequestException("'number' has to be numeric", e);
            }

            final int index = jsonArrayContent.getInt(2);

            return new UncleRequest.NumberAndIndex(number, index);
        } else if (typeStr.equals("number")) {

            if (jsonArrayContent.length() != 2) {
                throw new InvalidRequestException("Too much or not enough parameters");
            }

            if (!JSONUtil.hasString(jsonArrayContent, 1)) {
                throw new InvalidRequestException("'number' has to be string");
            }

            final BigInteger number;

            try {
                number = new BigInteger(jsonArrayContent.getString(1));
            } catch (NumberFormatException e) {
                throw new InvalidRequestException("'number' has to be numeric", e);
            }

            return new UncleRequest.Number(number);
        } else if (typeStr.equals("hash")) {
            if (jsonArrayContent.length() != 2) {
                throw new InvalidRequestException("Too much or not enough parameters");
            }

            if (!JSONUtil.hasString(jsonArrayContent, 1)) {
                throw new InvalidRequestException("'hash' has to be string");
            }

            if (!FormatValidator.isValidBlockHash(jsonArrayContent.getString(1))) {
                throw new InvalidRequestException("Invalid block hash");
            }

            return new UncleRequest.Hash(jsonArrayContent.getString(1));
        } else {
            throw new InvalidRequestException("Unknown type");
        }
    }

    @Override
    public Object handle(UncleRequest parameters) throws Exception {
        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            final PreparedStatement prpstmt;

            if (parameters instanceof UncleRequest.Hash) {
                prpstmt = connection.prepareStatement(
                        "SELECT " + UNCLE_COLUMNS + " FROM uncle_blocks WHERE hash = ? LIMIT 1");
                prpstmt.setString(1, ((UncleRequest.Hash) parameters).hash);
            } else if (parameters instanceof UncleRequest.Number) {
                prpstmt = connection.prepareStatement(
                        "SELECT " + UNCLE_COLUMNS + " FROM uncle_blocks WHERE number = ? LIMIT 1");
                prpstmt.setString(1, ((UncleRequest.Number) parameters).number.toString());
            } else {
                /* UncleRequest.NumberAndIndex */

                prpstmt = connection.prepareStatement("SELECT " + UNCLE_COLUMNS + " FROM uncle_blocks " +
                        "WHERE block_id = (SELECT blocks.internal_id FROM blocks WHERE blocks.number = ?) AND `index` = ? LIMIT 1");
                prpstmt.setString(1, ((UncleRequest.NumberAndIndex) parameters).number.toString());
                prpstmt.setInt(2, ((UncleRequest.NumberAndIndex) parameters).index);
            }

            final ResultSet resultSet = prpstmt.executeQuery();

            if (resultSet.next()) {
                final JSONObject jsonObjectUncle = getUncle(resultSet);

                prpstmt.close();

                return jsonObjectUncle;
            } else {
                prpstmt.close();

                return false;
            }
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

    static class UncleRequest {
        static class Hash extends UncleRequest {
            private final String hash;

            private Hash(String hash) {
                this.hash = hash;
            }
        }

        static class NumberAndIndex extends UncleRequest {
            private final BigInteger number;
            private final int index;

            private NumberAndIndex(BigInteger number, int index) {
                this.number = number;
                this.index = index;
            }
        }

        static class Number extends UncleRequest {
            private final BigInteger number;

            private Number(BigInteger number) {
                this.number = number;
            }
        }
    }
}
