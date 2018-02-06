package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;

import static net.nekonium.explorer.server.handler.HandlerCommon.*;

public class BlockListRequestHandler implements RequestHandler<BlockListRequestHandler.BlockListRequest> {

    private static final int ELEMENT_IN_PAGE = 25;

    @Override
    public BlockListRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        checkContentIsArray(jsonObject);

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        checkHasParameter(jsonArrayContent);
        final String typeStr = getString(jsonArrayContent, 0, "type");

        if (typeStr.equals("page")) {
            checkParamCount(jsonArrayContent, 2);

            final int pageNumber = parseNonNegativeInt(jsonArrayContent.get(1), "page_number");

            return new BlockListRequest.Page(pageNumber);
        } else if (typeStr.equals("last")) {
            checkParamCount(jsonArrayContent, 1);

            return new BlockListRequest.Last();
        } else {
            throw new InvalidRequestException("Unknown type");
        }
    }

    @Override
    public Object handle(BlockListRequest parameters) throws Exception {
        final JSONArray jsonArray = new JSONArray();

        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            // Get last page number from database

            final long approxRowCount = getApproximateRowCount(connection);      // Last page number
            final int lastPageNum = (int) ((approxRowCount - 1) / ELEMENT_IN_PAGE + 1);
            final int targetPageNum;

            if (parameters instanceof BlockListRequest.Page) {
                targetPageNum = ((BlockListRequest.Page) parameters).pageNumber;
            } else if (parameters instanceof BlockListRequest.Last) {
                targetPageNum = lastPageNum;
            } else {
                throw new InvalidRequestException("Invalid parameter type");
            }

            final JSONArray jsonArrayBlocks = querySearch(connection, targetPageNum, approxRowCount);

            jsonArray.put(jsonArrayBlocks);
            jsonArray.put(lastPageNum);

            return jsonArray;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred when closing an connection to database", e);
                }
            }
        }
    }

    private long getApproximateRowCount(Connection connection) throws SQLException {
        final Statement statement = connection.createStatement();

        // internal_id is AUTO_INCLEMENT so always number < internal_id, row count <= internal_id
        final ResultSet resultSet = statement.executeQuery("SELECT internal_id from blocks ORDER BY internal_id DESC LIMIT 1");
        resultSet.next();
        final long count = resultSet.getLong(1);

        resultSet.close();
        statement.close();

        return count;
    }

    private JSONArray querySearch(Connection connection, int targetPageNum, long approxRowCount) throws SQLException {
        final JSONArray jsonArray = new JSONArray();

        final PreparedStatement prpstmt = connection.prepareStatement(
                "SELECT blocks.internal_id, blocks.number, NEKH(blocks.hash), NEKH(addresses.address), " +
                        "(SELECT COUNT(*) FROM transactions WHERE transactions.block_id = blocks.internal_id), " +
                        "(SELECT COUNT(*) FROM uncle_blocks WHERE uncle_blocks.block_id = blocks.internal_id), " +
                        "blocks.forked, blocks.internal_id FROM blocks " +
                        "LEFT JOIN addresses ON addresses.internal_id = blocks.miner_id " +
                        "WHERE blocks.internal_id BETWEEN ? AND ? " +
                        "ORDER BY number DESC");
        prpstmt.setLong(1, approxRowCount - ELEMENT_IN_PAGE * targetPageNum + 1);   // Note: This is smaller than 👇
        prpstmt.setLong(2, approxRowCount - ELEMENT_IN_PAGE * (targetPageNum - 1)); // This is LARGER than 👆

        final ResultSet resultSet = prpstmt.executeQuery();

        while (resultSet.next()) {
            final JSONArray jsonArrayBlock = new JSONArray();

            jsonArrayBlock.put(resultSet.getString(1));                // Internal id
            jsonArrayBlock.put(resultSet.getString(2));                // Block number
            jsonArrayBlock.put(resultSet.getString(3));                // Hash
            jsonArrayBlock.put(resultSet.getString(4));                // Miner address
            jsonArrayBlock.put(resultSet.getInt(5));                   // Transaction count
            jsonArrayBlock.put(resultSet.getInt(6));                   // Uncle block count
            jsonArrayBlock.put(resultSet.getBoolean(7));               // Is forked

            jsonArray.put(jsonArrayBlock);
        }

        resultSet.close();
        prpstmt.close();

        return jsonArray;
    }

    static class BlockListRequest {

        static class Page extends BlockListRequest {

            private final int pageNumber;

            private Page(int pageNumber) {
                this.pageNumber = pageNumber;
            }
        }

        static class Last extends BlockListRequest {
        }
    }
}
