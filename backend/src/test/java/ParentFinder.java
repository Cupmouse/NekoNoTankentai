import net.nekonium.explorer.DatabaseManager;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ParentFinder {

    public static List<BigInteger> ids;

    public static void main(String[] args) {
        final DatabaseManager databaseManager = new DatabaseManager();

        Connection connection = null;

        try {
            databaseManager.init();

            connection = databaseManager.getConnection();

            final PreparedStatement prpstmtm1 = connection.prepareStatement("SELECT internal_id FROM blocks ORDER BY internal_id DESC");
            final ResultSet resultSet = prpstmtm1.executeQuery();

            resultSet.last();
            ids = new ArrayList<>(resultSet.getRow() + 10);

            while (resultSet.next()) {
                ids.add(new BigInteger(resultSet.getString(1)));
            }


            for (int i = ids.size() - 1; i >= 0; i--) {
                final BigInteger internalId = ids.get(i);

                connection.prepareStatement("UPDATE blocks SET parent = ");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
