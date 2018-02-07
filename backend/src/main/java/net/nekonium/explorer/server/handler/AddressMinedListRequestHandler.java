package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.util.NonNullPair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static net.nekonium.explorer.server.handler.HandlerCommon.*;

public class AddressMinedListRequestHandler implements RequestHandler<AddressMinedListRequestHandler.AddressMinedListRequest> {

    private static final int SEARCH_ELEMENTS_IN_PAGE = 25;
    private static final int SEARCH_ELEMENT_LIMIT = 1000;

    @Override
    public AddressMinedListRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        checkContentIsArray(jsonObject);

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        HandlerCommon.checkParamCount(jsonArrayContent, 2);

        final String hash = getAddressHash(jsonArrayContent, 0, "hash");
        final int pageNumber = parseUnsignedInt(jsonArrayContent.get(1), "pageNumber");

        if (pageNumber < 1) {
            throw new InvalidRequestException("'page_number' should be positive");
        }

        if (pageNumber * SEARCH_ELEMENTS_IN_PAGE > SEARCH_ELEMENT_LIMIT) {
            throw new InvalidRequestException("'page_number' exceeded server limit"); // Server limit exceeded
        }

        return new AddressMinedListRequest(hash.substring(2), pageNumber);
    }

    @Override
    public Object handle(AddressMinedListRequest parameters) throws Exception {
        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            final NonNullPair<Long, Integer> pair = getAddressIdAndApproximateRowCount(connection, parameters);
            if (pair == null) {
                return false;   // Address did not found
            }

            final long addressId = pair.getA();
            final int approximateRowCount = pair.getB();
            final int lastPageNumber = approximateRowCount / SEARCH_ELEMENTS_IN_PAGE + 1;

            final JSONArray jsonArray = new JSONArray();    // Result json structure root

            final JSONArray jsonArrayPage;

            if (parameters.pageNumber > lastPageNumber) {
                // No result expected, return empty result
                jsonArrayPage = new JSONArray();
            } else {
                // Get fee spent in each block
                final HashMap<String, BigInteger> txFeesInBlock = getTxFees(connection, parameters, addressId);

                // Get mined block/uncle blocks list from a database
                jsonArrayPage = makeMinedList(connection, parameters, txFeesInBlock, addressId);
            }

            jsonArray.put(jsonArrayPage);   // Put all result
            jsonArray.put(lastPageNumber);  // Add last page number

            final int foundRowCount;    // Optimized row count that actually returns back to client

            if (approximateRowCount > SEARCH_ELEMENT_LIMIT) {
                foundRowCount = SEARCH_ELEMENT_LIMIT + 1;
            } else {
                foundRowCount = approximateRowCount;
            }

            jsonArray.put(foundRowCount);   // Add it
            jsonArray.put(SEARCH_ELEMENT_LIMIT);

            return jsonArray;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e1) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred when closing an connection to a database");
                }
            }
        }
    }

    private NonNullPair<Long, Integer> getAddressIdAndApproximateRowCount(Connection connection, AddressMinedListRequest parameters) throws SQLException {
        final PreparedStatement prpstmt = connection.prepareStatement(
                "SELECT addresses.internal_id, " +
                        "(SELECT COUNT(*) FROM " +
                        "(SELECT 0 FROM blocks " +
                        "LEFT JOIN addresses a ON blocks.miner_id = a.internal_id " +
                        "WHERE a.address = UNHEX(?) LIMIT ?)" +
                        " AS a) + " +
                        "(SELECT COUNT(*) FROM " +
                        "(SELECT 0 FROM uncle_blocks " +
                        "LEFT JOIN addresses a ON uncle_blocks.miner_id = a.internal_id " +
                        "WHERE a.address = UNHEX(?) LIMIT ?)" +
                        " AS b) FROM addresses " +
                        "WHERE addresses.address = UNHEX(?)");
        prpstmt.setString(1, parameters.hash);
        prpstmt.setInt(2, SEARCH_ELEMENT_LIMIT + 1);
        prpstmt.setString(3, parameters.hash);
        prpstmt.setInt(4, SEARCH_ELEMENT_LIMIT + 1);
        prpstmt.setString(5, parameters.hash);

        final ResultSet resultSet = prpstmt.executeQuery();


        if (resultSet.next()) {
            final long addressId = resultSet.getLong(1);
            final int approximateRowCount = resultSet.getInt(2);

            resultSet.close();
            prpstmt.close();

            return new NonNullPair<>(addressId, approximateRowCount);
        } else {
            resultSet.close();
            prpstmt.close();

            return null;
        }
    }

    private JSONArray makeMinedList(Connection connection, AddressMinedListRequest parameters, HashMap<String, BigInteger> txFeesInBlock, long addressId) throws SQLException {
        final PreparedStatement prpstmt = connection.prepareStatement("(SELECT blocks.number, -1, " +
                "(SELECT COUNT(*) FROM uncle_blocks WHERE uncle_blocks.block_id = blocks.internal_id) FROM blocks " +
                "WHERE blocks.miner_id = ? " +
                "ORDER BY blocks.number DESC LIMIT ?)" +
                "UNION ALL " +
                "(SELECT b.number, uncle_blocks.number, uncle_blocks.`index` FROM uncle_blocks " +
                "LEFT JOIN blocks b ON b.internal_id = uncle_blocks.block_id " +
                "WHERE uncle_blocks.miner_id = ? " +
                "ORDER BY b.number DESC LIMIT ?) " +
                "ORDER BY number DESC LIMIT ? OFFSET ?");
        prpstmt.setLong(1, addressId);
        prpstmt.setInt(2, SEARCH_ELEMENT_LIMIT);
        prpstmt.setLong(3, addressId);
        prpstmt.setInt(4, SEARCH_ELEMENT_LIMIT);
        prpstmt.setInt(5, SEARCH_ELEMENTS_IN_PAGE);
        prpstmt.setInt(6, SEARCH_ELEMENTS_IN_PAGE * (parameters.pageNumber - 1));

        final ResultSet resultSet = prpstmt.executeQuery();

        final JSONArray jsonArrayPage = new JSONArray();

        while (resultSet.next()) {
            final JSONArray jsonArrayElem = new JSONArray();

            final String uncleBlockNumber = resultSet.getString(2);

            if (uncleBlockNumber.equals("-1")) {
                // Normal block
                jsonArrayElem.put("BLOCK"); // Type
                final String blockNumber;
                blockNumber = resultSet.getString(1);
                jsonArrayElem.put(blockNumber); // Block number
                jsonArrayElem.put(resultSet.getInt(2));    // Txs mined in a block FIXME maybe overflow if Nekonium grows unbelievably
                jsonArrayElem.put(getTxFeeOf(blockNumber, txFeesInBlock));
            } else {
                // Uncle block
                jsonArrayElem.put("UNCLE_BLOCK");   // Type
                jsonArrayElem.put(resultSet.getString(2)); // Mined block number
                jsonArrayElem.put(resultSet.getString(3)); // Uncle block number
                jsonArrayElem.put(resultSet.getInt(4));    // Uncle index in mined block
            }

            jsonArrayPage.put(jsonArrayElem);
        }
        return jsonArrayPage;
    }

    private HashMap<String, BigInteger> getTxFees(Connection connection, AddressMinedListRequest parameters, long addressId) throws SQLException {
        final HashMap<String, BigInteger> txFeesInBlock = new HashMap<>(SEARCH_ELEMENTS_IN_PAGE, 1);

        PreparedStatement prpstmt = connection.prepareStatement(
                "SELECT b.number, transactions.gas_used, transactions.gas_price FROM transactions " +
                        "LEFT JOIN blocks b ON b.internal_id = transactions.block_id " +
                        "WHERE b.miner_id = ? AND b.forked = 0 " +
                        "ORDER BY b.internal_id DESC " +
                        "LIMIT ? OFFSET ?");
        prpstmt.setLong(1, addressId);
        prpstmt.setInt(2, SEARCH_ELEMENTS_IN_PAGE);
        prpstmt.setInt(3, SEARCH_ELEMENTS_IN_PAGE * (parameters.pageNumber - 1));
        ResultSet resultSet = prpstmt.executeQuery();

        while (resultSet.next()) {
            // Calculate sum of tx fee for each block
            final String blockNumber = resultSet.getString(1);

            final BigInteger gasUsed = BigInteger.valueOf(resultSet.getLong(2));
            final BigInteger gasPrice = new BigInteger(resultSet.getString(3));

            final BigInteger feeSumBefore = txFeesInBlock.get(blockNumber);

            txFeesInBlock.put(blockNumber, feeSumBefore.add(gasPrice.multiply(gasUsed)));
        }

        resultSet.close();
        prpstmt.close();

        return txFeesInBlock;
    }

    private static BigInteger getTxFeeOf(String blockNumber, HashMap<String, BigInteger> txFeesInBlock) {
        final BigInteger bigInteger = txFeesInBlock.get(blockNumber);
        return bigInteger == null ? BigInteger.ZERO : bigInteger;
    }

    class AddressMinedListRequest {

        private String hash;
        private int pageNumber;

        private AddressMinedListRequest(String hash, int pageNumber) {
            this.hash = hash;
            this.pageNumber = pageNumber;
        }

        public int getPageNumber() {
            return pageNumber;
        }

        public String getHash() {
            return hash;
        }
    }
}
