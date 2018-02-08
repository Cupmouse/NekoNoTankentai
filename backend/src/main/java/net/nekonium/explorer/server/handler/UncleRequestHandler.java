package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.server.RequestHandlingException;
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

    // TODO Return forked or not

    @Override
    public UncleRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        checkContentIsArray(jsonObject);

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        checkHasParameter(jsonArrayContent);

        final String typeStr = getString(jsonArrayContent, 0, "type");

        if (typeStr.equals("hash-index")) {
            checkParamCount(jsonArrayContent, 3);

            final String blockHash = getString(jsonArrayContent, 1, "blockHash");
            if (!FormatValidateUtil.isValidBlockHash(blockHash)) {
                throw new InvalidRequestException("Invalid block hash");
            }
            final int uncleIndex = getUnsignedInt(jsonArrayContent, 1, "uncleIndex");

            return new UncleRequest.HashAndIndex(blockHash.substring(2), uncleIndex);

        } else if (typeStr.equals("number-index")) {
            checkParamCount(jsonArrayContent, 3);

            final BigInteger number = getNonNegativeBigInteger(jsonArrayContent, 1, "number");
            final int index = getUnsignedInt(jsonArrayContent, 2, "index");

            return new UncleRequest.NumberAndIndex(number, index);

        } else if (typeStr.equals("id-index")) {
            checkParamCount(jsonArrayContent, 3);

            final BigInteger blockId = getNonNegativeBigInteger(jsonArrayContent, 1, "blockId");
            final int index = getUnsignedInt(jsonArrayContent, 2, "index");

            return new UncleRequest.IdAndIndex(blockId, index);

        } else if (typeStr.equals("hash")) {
            checkParamCount(jsonArrayContent, 2);

            final String hash = getString(jsonArrayContent, 1, "hash");

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
                        "SELECT uncle_blocks.internal_id, uncle_blocks.number, NEKH(uncle_blocks.hash), " +
                                "UNIX_TIMESTAMP(uncle_blocks.timestamp), NEKH(addresses.address), " +
                                "uncle_blocks.difficulty, uncle_blocks.gas_limit, uncle_blocks.gas_used, NEKH(uncle_blocks.extra_data), " +
                                "uncle_blocks.nonce, uncle_blocks.size FROM uncle_blocks " +
                                "LEFT JOIN addresses ON uncle_blocks.miner_id = addresses.internal_id " +
                                "LEFT JOIN blocks ON blocks.internal_id = uncle_blocks.block_id " +
                                "WHERE uncle_blocks.hash = UNHEX(?) AND forked = 0 " +
                                "LIMIT 1");
                prpstmt.setString(1, ((UncleRequest.Hash) parameters).hash);

            } else if (parameters instanceof UncleRequest.IdAndIndex) {
                prpstmt = connection.prepareStatement(
                        "SELECT uncle_blocks.internal_id, number, NEKH(hash), UNIX_TIMESTAMP(timestamp), NEKH(address), " +
                                "difficulty, gas_limit, gas_used, NEKH(extra_data), nonce, size FROM uncle_blocks " +
                                "LEFT JOIN addresses ON uncle_blocks.miner_id = addresses.internal_id " +
                                "WHERE uncle_blocks.internal_id = ? AND uncle_blocks.`index` = ? " +
                                "LIMIT 1");
                prpstmt.setString(1, ((UncleRequest.IdAndIndex) parameters).id.toString());
                prpstmt.setInt(2, ((UncleRequest.IdAndIndex) parameters).index);

            } else if (parameters instanceof UncleRequest.NumberAndIndex) {
                prpstmt = connection.prepareStatement(
                        "SELECT uncle_blocks.internal_id, uncle_blocks.number, NEKH(uncle_blocks.hash), " +
                                "UNIX_TIMESTAMP(uncle_blocks.timestamp), NEKH(addresses.address), " +
                                "uncle_blocks.difficulty, uncle_blocks.gas_limit, uncle_blocks.gas_used, NEKH(uncle_blocks.extra_data), " +
                                "uncle_blocks.nonce, uncle_blocks.size FROM uncle_blocks " +
                                "LEFT JOIN addresses ON uncle_blocks.miner_id = addresses.internal_id " +
                                "LEFT JOIN blocks ON blocks.internal_id = uncle_blocks.block_id " +
                                "WHERE blocks.number = ? AND uncle_blocks.`index` = ? AND forked = 0 " +
                                "LIMIT 1");
                prpstmt.setString(1, ((UncleRequest.NumberAndIndex) parameters).number.toString());
                prpstmt.setInt(2, ((UncleRequest.NumberAndIndex) parameters).index);

            } else if (parameters instanceof UncleRequest.HashAndIndex) {
                prpstmt = connection.prepareStatement(
                        "SELECT uncle_blocks.internal_id, uncle_blocks.number, NEKH(uncle_blocks.hash), " +
                                "UNIX_TIMESTAMP(uncle_blocks.timestamp), NEKH(addresses.address), " +
                                "uncle_blocks.difficulty, uncle_blocks.gas_limit, uncle_blocks.gas_used, NEKH(uncle_blocks.extra_data), " +
                                "uncle_blocks.nonce, uncle_blocks.size FROM uncle_blocks " +
                                "LEFT JOIN addresses ON uncle_blocks.miner_id = addresses.internal_id " +
                                "LEFT JOIN blocks ON blocks.internal_id = uncle_blocks.block_id " +
                                "WHERE blocks.hash = UNHEX(?) AND uncle_blocks.`index` = ? AND forked = 0 " +
                                "LIMIT 1");
                prpstmt.setString(1, ((UncleRequest.HashAndIndex) parameters).hash);
                prpstmt.setInt(2, ((UncleRequest.HashAndIndex) parameters).index);

            } else {
                throw new RequestHandlingException("Unknown uncle request type");
            }

            final ResultSet resultSet = prpstmt.executeQuery();

            if (resultSet.next()) {
                final JSONArray jsonArray = parseUncleJSON(resultSet);

                prpstmt.close();

                return jsonArray;
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

    private static JSONArray parseUncleJSON(ResultSet resultSet) throws SQLException {
        final JSONArray jsonArray = new JSONArray();

        int n = 0;
        jsonArray.put(resultSet.getString(++n));
        jsonArray.put(resultSet.getString(++n));
        jsonArray.put(resultSet.getString(++n));
        jsonArray.put(resultSet.getLong(++n));
        jsonArray.put(resultSet.getString(++n));
        jsonArray.put(resultSet.getString(++n));
        jsonArray.put(resultSet.getLong(++n));
        jsonArray.put(resultSet.getLong(++n));
        jsonArray.put(resultSet.getString(++n));
        jsonArray.put(resultSet.getString(++n));
        jsonArray.put(resultSet.getInt(++n));

        return jsonArray;
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

        static class IdAndIndex extends UncleRequest {
            private final BigInteger id;
            private final int index;

            public IdAndIndex(BigInteger id, int index) {
                this.id = id;
                this.index = index;
            }
        }

        static class HashAndIndex extends UncleRequest {
            private final String hash;
            private final int index;

            public HashAndIndex(String hash, int index) {
                this.hash = hash;
                this.index = index;
            }
        }
    }
}
