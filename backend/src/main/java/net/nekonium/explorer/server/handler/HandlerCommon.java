package net.nekonium.explorer.server.handler;

import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

class HandlerCommon {
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
            jsonObjectTransaction.put("value",  resultSet.getBytes(++n));
        }

        jsonObjectTransaction.put("type",           transactionType.name().toLowerCase());

        jsonObjectTransaction.put("gas_provided",   resultSet.getInt(++n));
        jsonObjectTransaction.put("gas_used",       resultSet.getInt(++n));
        jsonObjectTransaction.put("gas_price",      new BigInteger(resultSet.getBytes(++n)).toString());
        jsonObjectTransaction.put("nonce",          resultSet.getString(++n));
        jsonObjectTransaction.put("input",          resultSet.getString(++n));

        return jsonObjectTransaction;
    }

    enum TransactionType {
        SENDING, CONTRACT_CREATION
    }
}
