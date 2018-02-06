package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import net.nekonium.explorer.server.handler.TransactionListRequestHandler.TransactionListRequest.AddressHash;
import net.nekonium.explorer.server.handler.TransactionListRequestHandler.TransactionListRequest.BlockHash;
import net.nekonium.explorer.server.handler.TransactionListRequestHandler.TransactionListRequest.BlockNumber;
import net.nekonium.explorer.util.NonNullPair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static net.nekonium.explorer.server.handler.HandlerCommon.*;

public class TransactionListRequestHandler implements RequestHandler<TransactionListRequestHandler.TransactionListRequest> {

    private static final int TX_LAST_PAGE_SEARCH_ELEM_LIMIT = 1000;
    private static final int ELEMENT_IN_PAGE = 25;

    @Override
    public TransactionListRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        HandlerCommon.checkContentIsArray(jsonObject);
        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        // Always 3 parameters
        HandlerCommon.checkParamCount(jsonArrayContent, 3);

        final String typeStr = getString(jsonArrayContent, 0, "type");

        final int pageNumber = parseNonNegativeInt(jsonArrayContent.get(2), "page_number");

        if (typeStr.equals("address-hash")) {
            final String addressHash = getAddressHash(jsonArrayContent, 1, "address_hash");

            return new AddressHash(addressHash.substring(2), pageNumber);

        } else if (typeStr.equals("block-hash")) {
            final String blockHash = getBlockHash(jsonArrayContent, 1, "block_hash");

            return new BlockHash(blockHash.substring(2), pageNumber);

        } else if (typeStr.equals("block-number")) {
            final BigInteger blockNumber = getNonNegativeBigInteger(jsonArrayContent, 1, "block_number");

            return new BlockNumber(blockNumber, pageNumber);

        } else {
            throw new InvalidRequestException("Unknown type");
        }
    }

    @Override
    public Object handle(TransactionListRequest parameters) throws Exception {
        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            // Find the last page number

            final int lastPageNumber = getLastPageNumber(connection, parameters);

            // Determine target page
            final int targetPageNumber;

            if (parameters.pageNumber == -1) {
                targetPageNumber = lastPageNumber;  // Request for last page
            } else {
                targetPageNumber = parameters.pageNumber;  // Request for specified page
            }

            // Get data from a database

            final JSONArray jsonArrayPage = querySearch(connection, parameters, targetPageNumber);

            // Create new array and put page array inside of it, also last page number and return them
            final JSONArray jsonArrayRsp = new JSONArray();

            jsonArrayRsp.put(jsonArrayPage);
            jsonArrayRsp.put(lastPageNumber);

            return jsonArrayRsp;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred when closing an connection", e);
                }
            }
        }
    }

    private int getLastPageNumber(Connection connection, TransactionListRequest parameters) throws SQLException, InvalidRequestException {
        final PreparedStatement prpstmt;

        if (parameters instanceof AddressHash) {
            final String addressHash = ((AddressHash) parameters).hash;

            prpstmt = connection.prepareStatement(
                    "SELECT COUNT(*) FROM (" +
                            "SELECT 1 FROM transactions " +
                            "LEFT JOIN blocks ON blocks.internal_id = transactions.block_id " +
                            "LEFT JOIN addresses AS a1 ON a1.internal_id = transactions.from_id " +
                            "LEFT JOIN addresses AS a2 ON a2.internal_id = transactions.to_id " +
                            "WHERE (a1.address = UNHEX(?) OR to_id = a2.address = UNHEX(?)) AND blocks.forked = 0 " +
                            "LIMIT ?) AS t");

            prpstmt.setString(1, addressHash);
            prpstmt.setString(2, addressHash);
            prpstmt.setInt(3, TX_LAST_PAGE_SEARCH_ELEM_LIMIT);

        } else if (parameters instanceof BlockHash) {
            final String blockHash = ((BlockHash) parameters).hash;

            // Could be forked block
            prpstmt = connection.prepareStatement(
                    "SELECT COUNT(*) FROM (" +
                            "SELECT 1 FROM transactions " +
                            "LEFT JOIN blocks ON blocks.internal_id = transactions.block_id " +
                            "WHERE blocks.hash = UNHEX(?) " +
                            "LIMIT ?) AS t");
            prpstmt.setString(1, blockHash);
            prpstmt.setInt(2,TX_LAST_PAGE_SEARCH_ELEM_LIMIT);

        } else if (parameters instanceof BlockNumber) {
            final BigInteger blockNumber = ((BlockNumber) parameters).blockNumber;

            prpstmt = connection.prepareStatement(
                    "SELECT COUNT(*) FROM (" +
                            "SELECT 1 FROM transactions " +
                            "LEFT JOIN blocks ON blocks.internal_id = transactions.block_id " +
                            "WHERE blocks.number = ? AND blocks.forked = 0 " +
                            "LIMIT ?) AS t");

            prpstmt.setString(1, blockNumber.toString());
            prpstmt.setInt(2, TX_LAST_PAGE_SEARCH_ELEM_LIMIT);
        } else {
            throw new InvalidRequestException("Unknown parameter type");
        }

        final ResultSet resultSet = prpstmt.executeQuery();

        resultSet.next();  // Expect only and least 1 row
        final int lastPageNumber = resultSet.getInt(1) / ELEMENT_IN_PAGE + 1;

        resultSet.close();
        prpstmt.close();

        return lastPageNumber;
    }

    private JSONArray querySearch(Connection connection, TransactionListRequest parameters, int targetPageNumber) throws SQLException, InvalidRequestException {
        final JSONArray jsonArrayPage = new JSONArray();    // All txs will be converted and stored here

        if (parameters instanceof AddressHash) {
            PreparedStatement prpstmt2 = connection.prepareStatement(
                    "SELECT NEKH(transactions.hash), blocks.number, UNIX_TIMESTAMP(blocks.timestamp), " +
                            "NEKH(a1.address), NEKH(a2.address), NEKH(a3.address), " +
                            "transactions.`value`, transactions.input = 0 FROM transactions " +
                            "LEFT JOIN addresses AS a1 ON a1.internal_id = transactions.from_id " +
                            "LEFT JOIN addresses AS a2 ON a2.internal_id = transactions.to_id " +
                            "LEFT JOIN addresses AS a3 ON a3.internal_id = transactions.contract_id " +
                            "LEFT JOIN blocks ON blocks.internal_id = transactions.block_id " +
                            "WHERE (a1.address = UNHEX(?) OR a2.address = UNHEX(?)) AND blocks.forked = 0 " +
                            "ORDER BY transactions.internal_id " +
                            "LIMIT ? OFFSET ?");

            prpstmt2.setString(1, ((AddressHash) parameters).hash);
            prpstmt2.setString(2, ((AddressHash) parameters).hash);
            prpstmt2.setInt(3, ELEMENT_IN_PAGE);
            prpstmt2.setInt(4, targetPageNumber);

            final ResultSet resultSet2 = prpstmt2.executeQuery();

            while (resultSet2.next()) {
                final JSONArray jsonArrayElem = new JSONArray();

                final String toAddress = resultSet2.getString(5);
                final String contractAddress = resultSet2.getString(6);
                final boolean emptyInput = resultSet2.getBoolean(8);

                final NonNullPair<TransactionType, String> typeAndTarget = determineTxTypeAndTargetAddress(toAddress, contractAddress, emptyInput);

                jsonArrayElem.put(typeAndTarget.getA().toString());         // Tx type
                jsonArrayElem.put(resultSet2.getString(1));     // Tx hash
                jsonArrayElem.put(resultSet2.getString(2));     // Block number
                jsonArrayElem.put(resultSet2.getLong(3));     // Timestamp
                jsonArrayElem.put(resultSet2.getString(4));     // From address
                jsonArrayElem.put(typeAndTarget.getB());                    // Traget
                jsonArrayElem.put(new BigInteger(resultSet2.getBytes(7)).toString());   // Value

                jsonArrayPage.put(jsonArrayElem);
            }

            resultSet2.close();
            prpstmt2.close();

        } else {
            // Request by block hash and number will return different result
            final PreparedStatement prpstmt2;

            if (parameters instanceof BlockHash) {
                prpstmt2 = connection.prepareStatement(
                        "SELECT NEKH(transactions.hash), NEKH(a1.address), NEKH(a2.address), NEKH(a3.address), " +
                                "transactions.`value`, transactions.input = 0 FROM transactions " +
                                "LEFT JOIN addresses AS a1 ON a1.internal_id = transactions.from_id " +
                                "LEFT JOIN addresses AS a2 ON a2.internal_id = transactions.to_id " +
                                "LEFT JOIN addresses AS a3 ON a3.internal_id = transactions.contract_id " +
                                "LEFT JOIN blocks ON blocks.internal_id = transactions.block_id " +
                                "WHERE blocks.hash = UNHEX(?) AND blocks.forked = 0 " +
                                "ORDER BY transactions.internal_id " +
                                "LIMIT ? OFFSET ?");
                prpstmt2.setString(1, ((BlockHash) parameters).hash);

            } else if (parameters instanceof BlockNumber) {
                prpstmt2 = connection.prepareStatement(
                        "SELECT NEKH(transactions.hash), NEKH(a1.address), NEKH(a2.address), NEKH(a3.address), " +
                                "transactions.`value`, transactions.input = 0 FROM transactions " +
                                "LEFT JOIN addresses AS a1 ON a1.internal_id = transactions.from_id " +
                                "LEFT JOIN addresses AS a2 ON a2.internal_id = transactions.to_id " +
                                "LEFT JOIN addresses AS a3 ON a3.internal_id = transactions.contract_id " +
                                "LEFT JOIN blocks ON blocks.internal_id = transactions.block_id " +
                                "WHERE blocks.number = ? AND blocks.forked = 0 " +
                                "ORDER BY transactions.`index` ASC " +
                                "LIMIT ? OFFSET ?");

                prpstmt2.setString(1, ((BlockNumber) parameters).blockNumber.toString());
            } else {
                throw new InvalidRequestException("Unknown parameter type");
            }

            final ResultSet resultSet2 = prpstmt2.executeQuery();

            // Convert all rows to tx json array object
            // Sorted by transaction index, ascending

            while (resultSet2.next()) {
                final JSONArray jsonArrayElem = new JSONArray();

                final String toAddress = resultSet2.getString(3);
                final String contractAddress = resultSet2.getString(4);
                final boolean emptyInput = resultSet2.getBoolean(6);

                final NonNullPair<TransactionType, String> typeAndTarget =
                        determineTxTypeAndTargetAddress(toAddress, contractAddress, emptyInput);

                jsonArrayElem.put(typeAndTarget.getA().toString());
                jsonArrayElem.put(resultSet2.getString(1)); // Transaction hash
                jsonArrayElem.put(resultSet2.getString(2));
                jsonArrayElem.put(typeAndTarget.getB());
                jsonArrayElem.put(new BigInteger(resultSet2.getBytes(5)).toString());

                jsonArrayPage.put(jsonArrayElem);
            }

            resultSet2.close();
            prpstmt2.close();
        }

        return jsonArrayPage;
    }

    static class TransactionListRequest {

        private final int pageNumber;

        private TransactionListRequest(int pageNumber) {
            this.pageNumber = pageNumber;
        }

        static final class BlockNumber extends TransactionListRequest {

            private final BigInteger blockNumber;

            private BlockNumber(BigInteger blockNumber, int pageNumber) {
                super(pageNumber);
                this.blockNumber = blockNumber;
            }
        }

        static final class BlockHash extends TransactionListRequest {

            private final String hash;

            private BlockHash(String hash, int pageNumber) {
                super(pageNumber);
                this.hash = hash;
            }
        }

        static final class AddressHash extends TransactionListRequest {

            private final String hash;

            private AddressHash(String hash, int pageNumber) {
                super(pageNumber);
                this.hash = hash;
            }
        }

    }
}
