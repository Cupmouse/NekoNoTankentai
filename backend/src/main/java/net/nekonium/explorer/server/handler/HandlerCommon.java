package net.nekonium.explorer.server.handler;

import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

class HandlerCommon {

    public static final String BLOCK_COLUMN = "internal_id, number, NEKH(hash), NEKH(parent_hash), UNIX_TIMESTAMP(timestamp), " +
            "NEKH(miner), difficulty, gas_limit, gas_used, NEKH(extra_data), nonce, NEKH(sha3_uncles), size";

    public static final String UNCLE_COLUMNS = "internal_id, number, NEKH(hash), NEKH(parent_hash), UNIX_TIMESTAMP(timestamp), NEKH(miner), " +
            "difficulty, gas_limit, gas_used, NEKH(extra_data), nonce, NEKH(sha3_uncles), size";

    public static final String TRANSACTION_COLUMN = "internal_id, NEKH(hash), NEKH(`from`), NEKH(`to`), NEKH(contract_address), value, gas_provided, " +
            "gas_used, gas_price, nonce, NEKH(input)";


    private HandlerCommon() {
    }

    static JSONObject getTransaction(ResultSet resultSet) throws SQLException {
        final JSONObject jsonObjectTransaction = new JSONObject();

        int n = 0;

        jsonObjectTransaction.put("internal_id",    resultSet.getString(++n));
        jsonObjectTransaction.put("hash",           resultSet.getString(++n));
        jsonObjectTransaction.put("from",           resultSet.getString(++n));

        TransactionType transactionType;

        if (resultSet.getString(++n) == null) { // to == null means contract creation transaction
            transactionType = TransactionType.CONTRACT_CREATION;

            jsonObjectTransaction.put("contract_address", resultSet.getString(++n));
            n++;    // Skip value
        } else {
            transactionType = TransactionType.SENDING;

            jsonObjectTransaction.put("to",     resultSet.getString(n));

            n++;    // Skip contract_address
            jsonObjectTransaction.put("value",  new BigInteger(resultSet.getBytes(++n)).toString());
        }

        jsonObjectTransaction.put("type",           transactionType.name().toLowerCase());

        jsonObjectTransaction.put("gas_provided",   resultSet.getInt(++n));
        jsonObjectTransaction.put("gas_used",       resultSet.getInt(++n));
        jsonObjectTransaction.put("gas_price",      new BigInteger(resultSet.getBytes(++n)).toString());
        jsonObjectTransaction.put("nonce",          resultSet.getString(++n));
        jsonObjectTransaction.put("input",          resultSet.getString(++n));

        return jsonObjectTransaction;
    }

    static JSONObject getUncle(ResultSet resultSet) throws SQLException {
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

    enum TransactionType {
        SENDING, CONTRACT_CREATION
    }
}
