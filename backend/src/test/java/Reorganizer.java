import net.nekonium.explorer.DatabaseManager;
import net.nekonium.explorer.util.IllegalDatabaseStateException;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Reorganizer {

    public static void main(String[] args) {
        BigInteger blockNumber = BigInteger.valueOf(678050);
        String parentHash = "0x419eb6dbc7fd1c6e4e0c80a03bd284126c668a58e873557ac9ea52e6da173707";

        final DatabaseManager databaseManager = new DatabaseManager();
        try {
            databaseManager.init();
        } catch (SQLException e) {
            return;
        }

        Connection connection = null;
        try {
            connection = databaseManager.getConnection();

            checkAndMarkForkedOneBack(connection, blockNumber, parentHash);
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

    public static void checkAndMarkForkedOneBack(Connection connection, BigInteger validBlockNumber, String validParent) throws SQLException {
        final BigInteger parentBlockNumber = validBlockNumber.subtract(BigInteger.ONE);

        /* Get parent block entry from the database */
        final PreparedStatement prpstmt = connection.prepareStatement("SELECT internal_id FROM blocks WHERE number = ? AND hash = UNHEX(?)");
        prpstmt.setString(1, parentBlockNumber.toString());
        prpstmt.setString(2, validParent.substring(2));

        final ResultSet resultSet = prpstmt.executeQuery();

        if (resultSet.last()) { // Move to last row
            if (resultSet.getRow() == 1) {  // If last row index was 1 then there is only one entry, thus it's a valid parent
                /* Mark "uncles" (NOT uncles included in blocks) as forked block */

                final String internalId = resultSet.getString(1);

                prpstmt.close();    // Don't forget to close the statement

                final PreparedStatement prpstmt2 = connection.prepareStatement("SELECT internal_id FROM blocks WHERE number = ? AND internal_id != ?");
                prpstmt2.setString(1, parentBlockNumber.toString());
                prpstmt2.setString(2, internalId);

                final ResultSet resultSet2 = prpstmt2.executeQuery();

                while (resultSet2.next()) {
                    connection.prepareStatement()

                    System.out.println(resultSet2.getString(1) + " IS forked block");
                }

                prpstmt2.close();
            } else {    // There are more than 2 blocks considered as valid parent (block hash is the same)
                prpstmt.close();
                throw new IllegalDatabaseStateException("Parent block with the same hash exist many [" + validParent + "]");
            }
        } else {    // Parent block doesn't recorded in the database, maybe converter missed it
            prpstmt.close();
            throw new IllegalDatabaseStateException("Unknown parent block hash, not recorded in the database [" + validParent + "]");
        }
    }
}
