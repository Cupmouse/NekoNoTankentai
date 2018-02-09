package net.nekonium.explorer.server.handler.search;

import net.nekonium.explorer.AddressType;
import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;

public class SearchRequestHandler implements RequestHandler<String> {

    @Override
    public String parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        if (jsonObject.get("content") instanceof String) {
            return jsonObject.getString("content");
        }

        throw new InvalidRequestException("Content must be string");
    }

    @Override
    public JSONArray handle(String searchWord) throws Exception {
        // TODO this thing is in WIP, now it's just checks if entered search word exists on the database
        // TODO maybe search a name tag of an address, a transaction and a contract, or a token name
        // TODO needs simple address or transaction format check, for saving database resources
        final ArrayList<SearchResultElement> results = new ArrayList<>();

        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            try {
                final BigInteger number = new BigInteger(searchWord);

                // This continues if word was complete numeric (number)
                // Comprehend word as block number

                addIfNotNull(results, searchAsBlockNumber(connection, number));
            } catch (NumberFormatException ignored) {
            }


            if (searchWord.length() == 64 + 2) {
                // Search for block

                addIfNotNull(results, searchAsBlockHash(connection, searchWord.substring(2)));

                // Might be a transaction

                addIfNotNull(results, searchAsTxHash(connection, searchWord.substring(2)));
            } else if (searchWord.length() == 40 + 2) {
                // Might be an address

                // Search for normal address first
                addIfNotNull(results, searchAsAddressHash(connection, searchWord.substring(2)));
            }
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred when closing a database connection", e);
                }
            }
        }

        final JSONArray jsonArray = new JSONArray();

        results.stream()
                .sorted(Comparator.comparingInt(SearchResultElement::getPriority).reversed()) // Descend ordered
                .map(SearchResultElement::toJSONArray)
                .forEach(jsonArray::put);

        return jsonArray;
    }

    private void addIfNotNull(ArrayList<SearchResultElement> results, SearchResultElement element) {
        if (element != null) {
            results.add(element);
        }
    }

    private SearchResultElementAddressHash searchAsAddressHash(Connection connection, String hexWithoutPrefix) throws SQLException {
        try (PreparedStatement prpstmt = connection.prepareStatement("SELECT type, NEKH(address), alias FROM addresses WHERE address = UNHEX(?) LIMIT 1")) {
            prpstmt.setString(1, hexWithoutPrefix);

            final ResultSet resultSet = prpstmt.executeQuery();

            if (resultSet.next()) {
                // Found it

                final AddressType addressType = AddressType.valueOf(resultSet.getString(1));// Address type
                final String addressHash = resultSet.getString(2);// Hash
                final String alias = resultSet.getString(3);// Alias

                return new SearchResultElementAddressHash(addressType, addressHash, alias);
            }

            return null;
        }
    }

    private SearchResultElementBlockNumber searchAsBlockNumber(Connection connection, BigInteger number) throws SQLException {
        try (PreparedStatement prpstmt = connection.prepareStatement("SELECT number FROM blocks WHERE number = ?")) {
            prpstmt.setString(1, number.toString());

            final ResultSet resultSet = prpstmt.executeQuery();

            if (resultSet.next()) {
                final BigInteger blockNumber = new BigInteger(resultSet.getString(1));

                return new SearchResultElementBlockNumber(blockNumber);
            }

            return null;
        }
    }

    private SearchResultElementTxHash searchAsTxHash(Connection connection, String hexWithoutPrefix) throws SQLException {
        try (PreparedStatement prpstmt = connection.prepareStatement("SELECT NEKH(hash) FROM transactions WHERE hash = UNHEX(?) LIMIT 1")) {
            prpstmt.setString(1, hexWithoutPrefix);

            final ResultSet resultSet = prpstmt.executeQuery();

            if (resultSet.next()) {
                final String txHash = resultSet.getString(1);

                return new SearchResultElementTxHash(txHash);
            }

            return null;
        }
    }

    private SearchResultElementBlockHash searchAsBlockHash(Connection connection, String hexWithoutPrefix) throws SQLException {
        try (PreparedStatement prpstmt = connection.prepareStatement("SELECT NEKH(hash) FROM blocks WHERE hash = UNHEX(?) LIMIT 1")) {
            prpstmt.setString(1, hexWithoutPrefix);

            final ResultSet resultSet = prpstmt.executeQuery();

            if (resultSet.next()) {
                final String blockHash = resultSet.getString(1);

                return new SearchResultElementBlockHash(blockHash);
            }

            return null;
        }
    }

}
