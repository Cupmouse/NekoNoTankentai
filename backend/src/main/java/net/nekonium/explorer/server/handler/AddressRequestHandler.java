package net.nekonium.explorer.server.handler;

import com.sun.tools.sjavac.server.RequestHandler;
import net.nekonium.explorer.server.ExplorerServer;
import net.nekonium.explorer.server.InvalidRequestException;
import net.nekonium.explorer.server.RequestHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.SQLException;

import static net.nekonium.explorer.server.handler.HandlerCommon.*;

public class AddressRequestHandler extends RequestHandler<AddressRequestHandler.AddressRequest> {


    @Override
    public AddressRequest parseParameters(JSONObject jsonObject) throws InvalidRequestException {
        checkContentIsArray(jsonObject);

        JSONArray jsonArrayContent = jsonObject.getJSONArray("content");

        checkHasParameter(jsonArrayContent);

        checkHasString(jsonArrayContent, 0, "type");
        String typeStr = jsonArrayContent.getString(0);

        if (typeStr.equals("hash")) {
            checkParamCount(jsonArrayContent, 2);

            checkHasString(jsonArrayContent, 1, "address_hash");

            String hash = jsonArrayContent.getString(1);

            return new AddressRequestHandler.AddressRequest.Hash(hash);
        } else {
            throw new InvalidRequestException("Unknown type");
        }
    }

    @Override
    public Object handle(AddressRequest parameters) throws Exception {
        if (parameters instanceof AddressRequest.Hash) {
            Connection connection = null;
            try {
                connection = ExplorerServer.getInstance().getBackend().getDatabaseManager().getConnection();

                connection.prepareStatement("SELECT address FROM addresses WHERE ");

                connection.prepareStatement("SELECT * FROM transactions " +
                        "LEFT JOIN addresses AS a1 ON address = UNHEX(?) " +
                        "LEFT JOIN addresses AS a2 ON address = UNHEX(?) " +
                        "WHERE from_id = a1.internal_id OR to_id = a2.internal_id");

            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        ExplorerServer.getInstance().getLogger().error("An error occurred when closing a connection", e);
                    }
                }
            }
        } else {
            throw new InvalidRequestException("Unknown type");
        }




        return null;
    }

    static class AddressRequest {

        static class Hash extends AddressRequest {

            private String hash;

            public Hash(String hash) {
                this.hash = hash;
            }
        }
    }
}
