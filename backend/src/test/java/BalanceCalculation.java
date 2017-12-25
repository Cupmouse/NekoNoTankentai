import net.nekonium.explorer.DatabaseManager;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BalanceCalculation {

    public static final BigInteger BLOCK_REWARD = BigInteger.valueOf(7500000000000000000L);

    public static void main(String[] args) {
        final String addr = "798C5f0816f83078Cdf2de43dB2509Dbac94B176";
        final DatabaseManager databaseManager = new DatabaseManager();

        Connection connection = null;

        try {
            databaseManager.init("jdbc:mysql://localhost:3306/boukentai", "root", "");

            connection = databaseManager.getConnection();

            // Receiving
            final PreparedStatement prpstmtr = connection.prepareStatement("SELECT `value` FROM transactions WHERE to_id = (SELECT internal_id FROM address WHERE address = UNHEX(?)) AND EXISTS(SELECT * FROM blocks WHERE blocks.internal_id = transactions.block_id AND forked = 0)");
            prpstmtr.setString(1, addr);
            final ResultSet resultSetR = prpstmtr.executeQuery();

            BigInteger rsum = BigInteger.ZERO;

            while (resultSetR.next()) {
                rsum = rsum.add(new BigInteger(resultSetR.getBytes(1)));
            }

            prpstmtr.close();

            // Sending
            final PreparedStatement prpstmts = connection.prepareStatement("SELECT `value`, gas_used, gas_price FROM transactions WHERE from_id = (SELECT internal_id FROM address WHERE address = UNHEX(?)) AND EXISTS(SELECT * FROM blocks WHERE blocks.internal_id = transactions.block_id AND forked = 0)");
            prpstmts.setString(1, addr);
            final ResultSet resultSetS = prpstmts.executeQuery();

            /* Subtracting sending fee */
            BigInteger ssum = BigInteger.ZERO;
            while (resultSetS.next()) {
                final BigInteger executionFee = new BigInteger(resultSetS.getString(2)).multiply(new BigInteger(resultSetS.getBytes(3)));

                ssum = ssum.subtract(new BigInteger(resultSetS.getBytes(1))).subtract(executionFee);
            }

            prpstmts.close();

            // Transaction fee return
            BigInteger tsum = BigInteger.ZERO;

            final PreparedStatement prpstmtt = connection.prepareStatement("SELECT transactions.gas_used, transactions.gas_price FROM transactions WHERE EXISTS (SELECT * FROM blocks WHERE blocks.internal_id = transactions.block_id AND blocks.miner_addr_id = (SELECT internal_id FROM address WHERE address = UNHEX(?)) AND forked = 0)");
            prpstmtt.setString(1, addr);

            final ResultSet resultSetT = prpstmtt.executeQuery();

            while (resultSetT.next()) {
                final BigInteger gas = BigInteger.valueOf(resultSetT.getLong(1));
                final BigInteger gasPrice = new BigInteger(resultSetT.getBytes(2));

                tsum = tsum.add(gas.multiply(gasPrice));
            }

            prpstmtt.close();

            // Normal block reward

            final PreparedStatement prpstmtb = connection.prepareStatement("SELECT 1 FROM blocks WHERE blocks.miner_addr_id = (SELECT internal_id FROM address WHERE address = UNHEX(?)) AND forked = 0");
            prpstmtb.setString(1, addr);
            final ResultSet resultSetB = prpstmtb.executeQuery();

            resultSetB.last();
            final int blockCount = resultSetB.getRow();

            final BigInteger bsum = BigInteger.valueOf(blockCount).multiply(BLOCK_REWARD);

            prpstmtb.close();

            // Uncle block reward
            final PreparedStatement prpstmtu = connection.prepareStatement("SELECT SUM(uncle_blocks.number + 8 - blocks.number) FROM uncle_blocks INNER JOIN blocks ON uncle_blocks.block_id = blocks.internal_id WHERE uncle_blocks.miner_id = (SELECT internal_id FROM address WHERE address = UNHEX(?)) AND blocks.forked = 0");
            prpstmtu.setString(1, addr);
            final ResultSet resultSetU = prpstmtu.executeQuery();

            final BigInteger usum;

            if (resultSetU.next() && resultSetU.getString(1) != null) {
                usum = new BigInteger(resultSetU.getString(1)).multiply(BLOCK_REWARD).divide(BigInteger.valueOf(8));
            } else {
                usum = BigInteger.ZERO;
            }

            prpstmtu.close();

            // Uncle including reward
            final PreparedStatement prpstmtm = connection.prepareStatement("SELECT SUM(" +
                    "(SELECT SUM(" +
                    "1" +
                    ") FROM uncle_blocks WHERE uncle_blocks.block_id = blocks.internal_id LIMIT 2)" +
                    ") FROM blocks WHERE blocks.miner_addr_id = (SELECT internal_id FROM address WHERE address = UNHEX(?)) AND blocks.forked = 0");
            prpstmtm.setString(1, addr);

            final ResultSet resultSetM = prpstmtm.executeQuery();

            final BigInteger msum;
            if (resultSetM.next() && resultSetM.getString(1) != null) {
                msum = new BigInteger(resultSetM.getString(1)).multiply(BLOCK_REWARD).divide(BigInteger.valueOf(32));
            } else {
                msum = BigInteger.ZERO;
            }


            prpstmtm.close();

            // Display

            final BigInteger sum = rsum.add(ssum).add(tsum).add(bsum).add(usum).add(msum);

            System.out.println(String.format("r: %s", new BigDecimal(rsum).movePointLeft(18).toString()));
            System.out.println(String.format("s: %s", new BigDecimal(ssum).movePointLeft(18).toString()));
            System.out.println(String.format("t: %s", new BigDecimal(tsum).movePointLeft(18).toString()));
            System.out.println(String.format("b: %s", new BigDecimal(bsum).movePointLeft(18).toString()));
            System.out.println(String.format("u: %s", new BigDecimal(usum).movePointLeft(18).toString()));
            System.out.println(String.format("m: %s", new BigDecimal(msum).movePointLeft(18).toString()));
            System.out.println(String.format("a: %s", new BigDecimal(sum).movePointLeft(18).toString()));
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

    public static BigInteger hexToBigInteger(String hex) {
        BigInteger rslt = BigInteger.ZERO;

        for (int i = 0; i < hex.length(); i++) {
            long v;

            switch (hex.charAt(hex.length() - i - 1)) {
                case '0': v = 0x0; break;
                case '1': v = 0x1; break;
                case '2': v = 0x2; break;
                case '3': v = 0x3; break;
                case '4': v = 0x4; break;
                case '5': v = 0x5; break;
                case '6': v = 0x6; break;
                case '7': v = 0x7; break;
                case '8': v = 0x8; break;
                case '9': v = 0x9; break;
                case 'A': v = 0xA; break;
                case 'B': v = 0xB; break;
                case 'C': v = 0xC; break;
                case 'D': v = 0xD; break;
                case 'E': v = 0xE; break;
                case 'F': v = 0xF; break;
                default:
                    return null;
            }

            rslt = rslt.add(BigInteger.valueOf(v).multiply(BigInteger.valueOf(0x10).pow(i)));
        }

        return rslt;
    }

}
