package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.util.FormatValidateUtil;
import net.nekonium.explorer.util.NonNullPair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

class HandlerCommon {

    public static final String UNCLE_COLUMNS = "internal_id, number, NEKH(hash), NEKH(parent_hash), UNIX_TIMESTAMP(timestamp), NEKH(miner), " +
            "difficulty, gas_limit, gas_used, NEKH(extra_data), nonce, NEKH(sha3_uncles), size";


    private HandlerCommon() {
    }

    static void checkContentIsArray(JSONObject jsonObject) throws InvalidRequestException {
        if (!(jsonObject.get("content") instanceof JSONArray)) {
            throw new InvalidRequestException("'content' has to be array");
        }
    }

    static void checkHasParameter(JSONArray jsonArrayContent) throws InvalidRequestException {
        if (jsonArrayContent.length() < 1) {
            throw new InvalidRequestException("No parameters found");
        }
    }

    static void checkParamCountAtLeast(JSONArray jsonArray, int count) throws InvalidRequestException {
        if (jsonArray.length() < count) {
            throw new InvalidRequestException("At least " + count + " parameters are expected");
        }
    }

    static void checkParamCount(JSONArray jsonArrayContent, int count) throws InvalidRequestException {
        if (jsonArrayContent.length() != count) {
            throw new InvalidRequestException("Too much element or not enough parameters (" + count + " expected)");
        }
    }

    static String getBlockHash(JSONArray jsonArray, int index, String name) throws InvalidRequestException {
        final String str = getString(jsonArray, index, name);// First, it needs to be string

        if (!FormatValidateUtil.isValidBlockHash(str)) {
            throw new InvalidRequestException("'" + name + "' has invalid block hash");
        }

        return str;
    }

    static String getAddressHash(JSONArray jsonArray, int index, String name) throws InvalidRequestException {
        final String str = getString(jsonArray, index, name);

        if (!FormatValidateUtil.isValidAddressHash(str)) {
            throw new InvalidRequestException("'" + name + "' has invalid address hash");
        }

        return str;
    }

    static void checkHasString(JSONArray jsonArrayContent, int index, String name) throws InvalidRequestException {
        if (!(jsonArrayContent.get(index) instanceof String)) {
            throw new InvalidRequestException("'" + name + "' have to be string");
        }
    }

    static String getString(JSONArray jsonArray, int index, String name) throws InvalidRequestException {
        final Object o = jsonArray.get(index);

        if (!(o instanceof String)) {
            throw new InvalidRequestException("'" + name + "' have to be string");
        }

        return (String) o;
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

    static BigInteger getNonNegativeBigInteger(JSONArray jsonArray, int count, String name) throws InvalidRequestException {
        checkHasString(jsonArray, count, name);

        return parseNonNegativeBigInteger(jsonArray.getString(count), name);
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

    static NonNullPair<TransactionType, String> determineTxTypeAndTargetAddress(String toAddress, String contractAddress, boolean emptyInput) throws InvalidRequestException {
        final TransactionType txType;
        final String targetAddress;

        if (toAddress != null) {
            targetAddress = toAddress;

            if (emptyInput) {
                // Consider this tx as a normal sending
                txType = TransactionType.SEND;
            } else {
                // Contract execution
                txType = TransactionType.CONTRACT_CALL;
            }
        } else if (contractAddress != null) {
            targetAddress = contractAddress;
            txType = TransactionType.CONTRACT_CREATION;
        } else {
            throw new InvalidRequestException("Unknown transaction type");
        }

        return new NonNullPair<>(txType, targetAddress);
    }

    enum TransactionType {
        SEND, CONTRACT_CREATION, CONTRACT_CALL
    }
}
