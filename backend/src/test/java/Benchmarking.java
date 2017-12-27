import net.nekonium.explorer.DatabaseManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Benchmarking {

    public static void main(String[] args) {
        final DatabaseManager databaseManager = new DatabaseManager();

        Connection connection = null;

        try {
            databaseManager.init("jdbc:mysql://localhost:3306/boukentai", "root", "");

            connection = databaseManager.getConnection();


            long startTime = System.currentTimeMillis();


            final PreparedStatement prpstmt = connection.prepareStatement("SELECT internal_id, number, NEKH(hash),  NEKH((SELECT hash FROM blocks AS t WHERE t.internal_id = blocks.parent)), UNIX_TIMESTAMP(timestamp), NEKH((SELECT address FROM addresses WHERE addresses.internal_id = blocks.miner_id)), difficulty, gas_limit, gas_used, NEKH(extra_data), nonce, size FROM blocks WHERE number = ? LIMIT 1");
            for (int i = 0; i <= 100000; i++) {
                prpstmt.setString(1, String.valueOf(i));

                prpstmt.executeQuery();
            }
            prpstmt.close();

            System.out.println("Time : " + (System.currentTimeMillis() - startTime));

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
