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

            final int lastPageNum = getLastPageNum(connection);      // Last page number
            final int targetPageNum;

            if (parameters instanceof BlockListRequest.Page) {
                targetPageNum = ((BlockListRequest.Page) parameters).pageNumber;
            } else if (parameters instanceof BlockListRequest.Last) {
                targetPageNum = lastPageNum;
            } else {
                throw new InvalidRequestException("Invalid parameter type");
            }

            final JSONArray jsonArrayBlocks = querySearch(connection, targetPageNum);

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

    private int getLastPageNum(Connection connection) throws SQLException {
        int lastPageNum;
        final Statement statement = connection.createStatement();

        final ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM blocks");
        resultSet.next();
        final long count = resultSet.getLong(1);

        resultSet.close();
        statement.close();

        lastPageNum = (int) ((count - 1) / ELEMENT_IN_PAGE + 1);
        return lastPageNum;
    }

    private JSONArray querySearch(Connection connection, int targetPageNum) throws SQLException {
        final JSONArray jsonArray = new JSONArray();

        final PreparedStatement prpstmt = connection.prepareStatement(
                "SELECT blocks.number, NEKH(blocks.hash), NEKH(addresses.address), " +
                        "(SELECT COUNT(*) FROM transactions WHERE transactions.block_id = blocks.internal_id), " +
                        "(SELECT COUNT(*) FROM uncle_blocks WHERE uncle_blocks.block_id = blocks.internal_id), " +
                        "blocks.forked, blocks.internal_id FROM blocks " +
                        "LEFT JOIN addresses ON addresses.internal_id = blocks.miner_id " +
                        "WHERE blocks.internal_id " +
                        "ORDER BY number DESC LIMIT ? OFFSET ?");
        prpstmt.setLong(1, ELEMENT_IN_PAGE);
        prpstmt.setLong(2, ELEMENT_IN_PAGE * (targetPageNum - 1));

        final ResultSet resultSet = prpstmt.executeQuery();

        while (resultSet.next()) {
            final JSONArray jsonArrayBlock = new JSONArray();

            jsonArrayBlock.put(resultSet.getString(1));                // Block Number
            jsonArrayBlock.put(resultSet.getString(2));                // Hash
            jsonArrayBlock.put(resultSet.getString(3));                // Miner address
            jsonArrayBlock.put(resultSet.getInt(4));                   // Transaction count
            jsonArrayBlock.put(resultSet.getInt(5));                   // Uncle block count
            jsonArrayBlock.put(resultSet.getBoolean(6));               // Is forked

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
