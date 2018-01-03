package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.InvalidRequestException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

class HandlerCommon {

    private HandlerCommon() {
    }

    static void checkContentIsArray(JSONObject jsonObject) throws InvalidRequestException {
        if (!hasJSONArray(jsonObject, "content")) {
            throw new InvalidRequestException("'content' has to be array");
        }
    }

    static void checkHasParameter(JSONArray jsonArrayContent) throws InvalidRequestException {
        if (jsonArrayContent.length() < 1) {
            throw new InvalidRequestException("No parameters found");
        }
    }

    static void checkParamCount(JSONArray jsonArrayContent, int count) throws InvalidRequestException {
        if (jsonArrayContent.length() != count) {
            throw new InvalidRequestException("Too much element or not enough parameters (" + count + " expected)");
        }
    }

    static void checkHasString(JSONArray jsonArrayContent, int index, String name) throws InvalidRequestException {
        if (!hasString(jsonArrayContent, index)) {
            throw new InvalidRequestException("'" + name + "' have to be string");
        }
    }

    static BigInteger parseNonNegativeBigInteger(String str, String name) throws InvalidRequestException {
        final BigInteger bigInteger;

        try {
            bigInteger = new BigInteger(str);
        } catch (NumberFormatException e) {
            throw new InvalidRequestException("'" + name + "' has to be numeric", e);
        }

        if (bigInteger.compareTo(BigInteger.ZERO) < 0) {
            throw new InvalidRequestException("'" + name + "' cannot be negative");
        }
        return bigInteger;
    }

    static int parseNonNegativeInt(Object o, String name) throws InvalidRequestException {
        if (!(o instanceof Number)) {
            throw new InvalidRequestException("'" + name + "' has to be numeric");
        }

        final int result = ((Number) o).intValue();

        if (result < 0) {
            throw new InvalidRequestException("'" + name + "' cannot be negative");
        }

        return result;
    }

    static JSONObject parseUncleJSON(ResultSet resultSet) throws SQLException {
        final JSONObject jsonObjectUncle = new JSONObject();

        int n = 0;
        jsonObjectUncle.put("internal_id"   , resultSet.getString(++n));
        jsonObjectUncle.put("number"        , resultSet.getString(++n));
        jsonObjectUncle.put("hash"          , resultSet.getString(++n));
        jsonObjectUncle.put("parent_hash"   , resultSet.getString(++n));
        jsonObjectUncle.put("timestamp"     , resultSet.getLong(++n));
        jsonObjectUncle.put("miner"         , resultSet.getString(++n));
        jsonObjectUncle.put("difficulty"    , resultSet.getString(++n));
        jsonObjectUncle.put("gas_limit"     , resultSet.getLong(++n));
        jsonObjectUncle.put("gas_used"      , resultSet.getLong(++n));
        jsonObjectUncle.put("extra_data"    , resultSet.getString(++n));
        jsonObjectUncle.put("nonce"         , resultSet.getString(++n));
        jsonObjectUncle.put("sha3_uncles"   , resultSet.getString(++n));
        jsonObjectUncle.put("size"          , resultSet.getInt(++n));
        return jsonObjectUncle;
    }

    public static boolean hasTyped(final JSONArray jsonArray, final int index, final Class<?> type) throws JSONException {
        return type.isInstance(jsonArray.get(index));
    }

    public static boolean hasString(final JSONArray jsonArray, final int index) throws JSONException {
        return jsonArray.get(index) instanceof String;
    }

    public static boolean hasNumber(final JSONArray jsonArray, final int index) throws JSONException {
        return jsonArray.get(index) instanceof Number;
    }

    public static boolean hasJSONObject(final JSONObject jsonObject, final String key) throws JSONException {
        return jsonObject.get(key) instanceof JSONObject;
    }

    public static boolean hasJSONArray(final JSONObject jsonObject, final String key) throws JSONException {
        return jsonObject.get(key) instanceof JSONArray;
    }

    enum TransactionType {
        SENDING, CONTRACT_CREATION, CONTRACT_CALL
    }
}
