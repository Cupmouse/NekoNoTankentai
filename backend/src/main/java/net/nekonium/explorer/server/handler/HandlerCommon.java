package net.nekonium.explorer.server.handler;

import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.function.Function;

class HandlerCommon {

    public static final String UNCLE_COLUMNS = "internal_id, number, NEKH(hash), NEKH(parent_hash), UNIX_TIMESTAMP(timestamp), NEKH(miner), " +
            "difficulty, gas_limit, gas_used, NEKH(extra_data), nonce, NEKH(sha3_uncles), size";


    private HandlerCommon() {
    }

    public static void writeTx(JSONObject jsonObject,
                               long internalId, long blockId, long blockNumber, int index, String hash,
                               String from, String to, String contract, BigInteger value,
                               long gasProvided, long gasUsed, BigInteger gasPrice, String nonce, String input) {
        jsonObject.put("internal_id",   internalId);
        jsonObject.put("block_id",      blockId);
        jsonObject.put("block_number",  blockNumber);
        jsonObject.put("index",         index);
        jsonObject.put("hash",          hash);
        jsonObject.put("from",          from);
        jsonObject.put("to",            to);
        jsonObject.put("contract",      contract);
        jsonObject.put("value",         value);
        jsonObject.put("gas_provided",  gasProvided);
        jsonObject.put("gas_used",      gasUsed);
        jsonObject.put("gas_price",     gasPrice);
        jsonObject.put("nonce",         nonce);
        jsonObject.put("input",         input);
    }

    // TODO Change type here if extra space for number or internal_id is needed

    public static void writeBlock(JSONObject jsonObject,
                      long internalId, long number, String hash, String parentHash, long timestamp, String miner,
                      String difficulty, long gasLimit, long gasUsed, String extraData, String nonce,
                      int size, boolean forked) {
        jsonObject.put("internal_id",   internalId);
        jsonObject.put("number",        number);
        jsonObject.put("hash",          hash);
        jsonObject.put("parentHash",    parentHash);
        jsonObject.put("timestamp",     timestamp);
        jsonObject.put("miner",         miner);
        jsonObject.put("difficulty",    difficulty);
        jsonObject.put("gas_limit",     gasLimit);
        jsonObject.put("gas_used",      gasUsed);
        jsonObject.put("extra_data",    extraData);
        jsonObject.put("nonce",         nonce);
        jsonObject.put("size",          size);
        jsonObject.put("forked",        forked);
    }

    static Object getTransaction(final Connection connection, final String condition, PrepareFeeder paramFeeder, boolean returnArray) throws SQLException {
        final PreparedStatement prpstmt = connection.prepareStatement(
                "SELECT transactions.internal_id, transactions.block_id, blocks.number, transactions.`index`, NEKH(transactions.hash), " +
                        "NEKH((SELECT address FROM addresses WHERE addresses.internal_id = transactions.from_id)), " +
                        "NEKH((SELECT address FROM addresses WHERE addresses.internal_id = transactions.to_id)), " +
                        "NEKH((SELECT address FROM addresses WHERE addresses.internal_id = transactions.contract_id)), " +
                        "transactions.`value`, transactions.gas_provided, transactions.gas_used, transactions.gas_price, " +
                        "transactions.nonce, NEKH(transactions.input) " +

                        "FROM transactions " +
                        "LEFT JOIN blocks ON blocks.internal_id = transactions.block_id " +

                        condition);



        try {
            paramFeeder.feed(prpstmt);// Feeds the parameters

            final ResultSet resultSet = prpstmt.executeQuery();

            if (returnArray) {
                // Return array of txs if there are more or equal than 1, empty array if there is none
                JSONArray txArray = new JSONArray();

                while (resultSet.next()) {
                    txArray.put(formatJSONTx(resultSet));
                }

                return txArray;
            } else {
                // Returns top of the results, if there is none, false

                if (resultSet.next()) {
                    return formatJSONTx(resultSet);
                } else {
                    return false;
                }
            }

        } finally {
            prpstmt.close();    // This will sure close statement
        }
    }

    private static JSONObject formatJSONTx(ResultSet resultSet) throws SQLException {
        final JSONObject jsonObjectTransaction = new JSONObject();

        int n = 0;

        jsonObjectTransaction.put("internal_id",    resultSet.getString(++n));
        jsonObjectTransaction.put("block_id",       resultSet.getString(++n));
        jsonObjectTransaction.put("block_number",   resultSet.getString(++n));
        jsonObjectTransaction.put("index",          resultSet.getString(++n));
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

    enum TransactionType {
        SENDING, CONTRACT_CREATION
    }

    public interface PrepareFeeder {

        void feed(final PreparedStatement prpstmt) throws SQLException;
    }
}
