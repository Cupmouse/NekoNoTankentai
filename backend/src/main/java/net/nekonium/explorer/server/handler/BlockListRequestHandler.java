package net.nekonium.explorer.server.handler;

import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigInteger;
import java.sql.*;

import static net.nekonium.explorer.server.handler.HandlerCommon.*;

public class BlockListRequestHandler implements RequestHandler<BlockListRequestHandler.BlockListRequest> {

    private static final int ELEMENT_IN_PAGE = 25;

    @Override
    public BlockListRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        checkContentIsArray(jsonObject);

        final JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        checkHasParameter(jsonArrayContent);
        checkHasString(jsonArrayContent, 0, "type");

        final String typeStr = jsonArrayContent.getString(0);

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
        final JSONArray jsonArrayBlocks = new JSONArray();

        final int targetPageNum;    // Target page number
        final int lastPageNum;      // Last page number

        Connection connection = null;

        try {
            connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

            // Get last page number from database

            final Statement statement = connection.createStatement();

            final ResultSet resultSet1 = statement.executeQuery("SELECT COUNT(*) FROM blocks");
            final long count = resultSet1.getLong(1);

            statement.close();

            lastPageNum = (int) ((count - 1) / ELEMENT_IN_PAGE + 1);

            if (parameters instanceof BlockListRequest.Page) {
                targetPageNum = ((BlockListRequest.Page) parameters).pageNumber;
            } else if (parameters instanceof BlockListRequest.Last) {
                targetPageNum = lastPageNum;
            } else {
                throw new InvalidRequestException("Invalid parameter type");
            }

            final PreparedStatement prpstmt = connection.prepareStatement(
                        "SELECT blocks.number, NEKH(blocks.hash), NEKH(addresses.address), " +
                                "(SELECT COUNT(*) FROM transactions WHERE transactions.block_id = blocks.internal_id), " +
                                "(SELECT COUNT(*) FROM uncle_blocks WHERE uncle_blocks.block_id = blocks.internal_id), " +
                                "blocks.forked, blocks.internal_id FROM blocks " +
                                "LEFT JOIN addresses ON addresses.internal_id = blocks.miner_id " +
                                "WHERE blocks.internal_id BETWEEN ? AND ? " +
                                "ORDER BY number DESC");
            prpstmt.setLong(1, ELEMENT_IN_PAGE * targetPageNum);
            prpstmt.setLong(2, ELEMENT_IN_PAGE * (targetPageNum + 1) - 2);

            final ResultSet resultSet2 = prpstmt.executeQuery();

            while (resultSet2.next()) {
                final JSONArray jsonArrayBlock = new JSONArray();

                jsonArrayBlock.put(new BigInteger(resultSet2.getString(1))); // Block Number
                jsonArrayBlock.put(resultSet2.getString(2));                 // Hash
                jsonArrayBlock.put(resultSet2.getString(3));                 // Miner address
                jsonArrayBlock.put(resultSet2.getInt(4));                    // Transaction count
                jsonArrayBlock.put(resultSet2.getInt(5));                    // Uncle block count
                jsonArrayBlock.put(resultSet2.getBoolean(6));                // Is forked

                jsonArrayBlocks.put(jsonArrayBlock);
            }

            prpstmt.close();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    ExplorerServer.getInstance().getLogger().error("An error occurred when closing an connection to database", e);
                }
            }
        }

        jsonArray.put(jsonArrayBlocks);
        jsonArray.put(lastPageNum);

        return jsonArray;
    }

    static class BlockListRequest {

        static class Page extends BlockListRequest {

            private int pageNumber;

            private Page(int pageNumber) {
                this.pageNumber = pageNumber;
            }
        }

        static class Last extends BlockListRequest {
        }
    }
}
