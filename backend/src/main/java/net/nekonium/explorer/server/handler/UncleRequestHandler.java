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

public class UncleRequestHandler implements RequestHandler<UncleRequestHandler.UncleRequest> {

    @Override
    public UncleRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        checkContentIsArray(jsonObject);

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        checkHasParameter(jsonArrayContent);
        checkHasString(jsonArrayContent, 0, "type");

        final String typeStr = jsonArrayContent.getString(0);

        if (typeStr.equals("number_and_index")) {

            checkParamCount(jsonArrayContent, 3);

            checkHasString(jsonArrayContent, 1, "number");
            final BigInteger number = parseNonNegativeBigInteger(jsonArrayContent.getString(1), "number");

            final int index = parseUnsignedInt(jsonArrayContent.get(2), "index");

            return new UncleRequest.NumberAndIndex(number, index);
        } else if (typeStr.equals("id_and_index")) {

            checkParamCount(jsonArrayContent, 3);

            checkHasString(jsonArrayContent, 1, "number");
            final BigInteger number = parseNonNegativeBigInteger(jsonArrayContent.getString(1), "number");

            final int index = parseUnsignedInt(jsonArrayContent.get(2), "index");

            return new UncleRequest.Number(number);
        } else if (typeStr.equals("hash")) {

            checkParamCount(jsonArrayContent, 2);
            checkHasString(jsonArrayContent, 1, "hash");

            final String hash = jsonArrayContent.getString(1);

            if (!FormatValidateUtil.isValidBlockHash(hash)) {
                throw new InvalidRequestException("Invalid block hash");
            }

            return new UncleRequest.Hash(hash.substring(2));
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
                        "SELECT " + UNCLE_COLUMNS + " FROM uncle_blocks WHERE hash = UNHEX(?) LIMIT 1");
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
                final JSONObject jsonObjectUncle = parseUncleJSON(resultSet);

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
