package net.nekonium.explorer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Transaction;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;

import java.math.BigInteger;
import java.sql.*;

import static java.sql.Statement.RETURN_GENERATED_KEYS;

public class BlockchainConverter implements Runnable {

    private final Web3jManager web3jManager;
    private final Logger logger;
    private HikariDataSource dataSource;

    public BlockchainConverter(Web3jManager web3jManager) {
        this.web3jManager = web3jManager;
        this.logger = LoggerFactory.getLogger("Converter");
    }

    public void init() throws SQLException {
        final HikariConfig config = new HikariConfig();

        config.setJdbcUrl("jdbc:mysql://localhost:3306/nek_blockchain");
        config.setUsername("root");
        config.setPassword("");

        this.dataSource = new HikariDataSource(config);
        // 接続してみる
        this.dataSource.getConnection();
    }

    @Override
    public void run() {
        try {
            final Connection connection = dataSource.getConnection();

            // Set auto-commit off
            connection.setAutoCommit(false);

            logger.info("Getting the latest block number have been fetched last time from database...");

            // Lets check block number where to start fetching from
            final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery("SELECT number FROM blocks ORDER BY number DESC LIMIT 1");

            long catchupFetchStart;
            if (resultSet.next()) {
                catchupFetchStart = resultSet.getLong(1) + 1;
            } else {
                // Found no data on the database, fetching blockchain data from the block 0
                catchupFetchStart = 0;
            }

            statement.close();
            connection.close();

            // Fetching all block data from catchupFetchStart to the latest block
            this.logger.info("Catching up fetching is starting from block #" + catchupFetchStart);

            final Subscription subscribe = this.web3jManager.getWeb3j()
                    .catchUpToLatestBlockObservable(DefaultBlockParameter.valueOf(BigInteger.valueOf(catchupFetchStart)), true)
                    .subscribe(new ConverterRunner());

            this.logger.info("Catching up...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private class ConverterRunner implements Action1<EthBlock> {

        @Override
        public void call(EthBlock ethBlock) {
            Connection connection = null;

            try {
                // Get a connection from pool
                connection = dataSource.getConnection();

                // Get a block response
                final EthBlock.Block block = ethBlock.getBlock();

                logger.info("Got new block from node : #" + block.getNumber());

                // Generating json string for database storing TODO its not needed can be deleted
                final JSONArray jsonArrayTransactions = new JSONArray();
                block.getTransactions().stream()
                        .map(transactionResult -> ((EthBlock.TransactionObject) transactionResult).get().getHash())
                        .forEach(jsonArrayTransactions::put);
                final String jsonTransactionHashes = jsonArrayTransactions.toString();

                final JSONArray jsonArrayUncles = new JSONArray();
                block.getUncles().forEach(jsonArrayUncles::put);
                final String jsonUncles = jsonArrayUncles.toString();

                // First, insert the block
                final PreparedStatement prpstmt = connection.prepareStatement(
                        "INSERT INTO blocks VALUES " +
                                "(?, UNHEX(?), UNHEX(?), FROM_UNIXTIME(?), UNHEX(?), ?, ?, ?, UNHEX(?), ?, " +
                                "UNHEX(?), ?, UNHEX(?), UNHEX(?), UNHEX(?), UNHEX(?), UNHEX(?), ?, ?, ?)");

                // TODO Every integer number on go-nekonium is non-fixed integer, it will overflow on mysql in future (distant future)
                prpstmt.setString(1, block.getNumber().toString());
                prpstmt.setString(2, block.getHash().substring(2));
                prpstmt.setString(3, block.getParentHash().substring(2));
                prpstmt.setString(4, block.getTimestamp().toString());
                prpstmt.setString(5, block.getMiner().substring(2));
                prpstmt.setString(6, block.getDifficulty().toString());
                prpstmt.setString(7, block.getGasLimit().toString());
                prpstmt.setString(8, block.getGasUsed().toString());
                prpstmt.setString(9, block.getExtraData().substring(2));
                prpstmt.setString(10, block.getNonce().toString());
                prpstmt.setString(11, block.getSha3Uncles().substring(2));
                prpstmt.setString(12, block.getSize().toString());
                prpstmt.setString(13, block.getLogsBloom().substring(2));
                prpstmt.setString(14, block.getMixHash().substring(2));
                prpstmt.setString(15, block.getReceiptsRoot().substring(2));
                prpstmt.setString(16, block.getStateRoot().substring(2));
                prpstmt.setString(17, block.getTransactionsRoot().substring(2));
                prpstmt.setBytes(18, block.getTotalDifficulty().toByteArray());
                prpstmt.setString(19, jsonTransactionHashes);
                prpstmt.setString(20, jsonUncles);

                prpstmt.executeUpdate();
                prpstmt.close();

                // Second, insert transactions

                for (EthBlock.TransactionResult result : block.getTransactions()) {
                    final Transaction transaction = (Transaction) result.get();

                    final PreparedStatement prpstmt2 = connection.prepareStatement(
                            "INSERT INTO transactions VALUES " +
                                    "(?, ?, UNHEX(?), UNHEX(?), UNHEX(?), ?, ?, ?, ?, UNHEX(?), " +
                                    "UNHEX(?), UNHEX(?), ?)");
                    prpstmt2.setString(1, transaction.getBlockNumber().toString());
                    prpstmt2.setString(2, transaction.getTransactionIndex().toString());
                    prpstmt2.setString(3, transaction.getHash().substring(2));
                    prpstmt2.setString(4, transaction.getFrom().substring(2));
                    prpstmt2.setString(5, transaction.getTo() == null ? null : transaction.getTo().substring(2));
                    prpstmt2.setBytes(6, transaction.getValue().toByteArray());
                    prpstmt2.setString(7, transaction.getGas().toString());
                    prpstmt2.setBytes(8, transaction.getGasPrice().toByteArray());
                    prpstmt2.setString(9, transaction.getNonce().toString());
                    prpstmt2.setString(10, transaction.getInput().substring(2));
                    prpstmt2.setString(11, transaction.getR().substring(2));
                    prpstmt2.setString(12, transaction.getS().substring(2));
                    prpstmt2.setInt(13, transaction.getV());
                    prpstmt2.executeUpdate();
                    prpstmt2.close();
                }

                // And finally, commit it
                connection.commit();
            } catch (Exception e) {
                logger.error("Database issue! Stopping fetching", e);

                if (connection != null) {
                    try {
                        connection.rollback();
                    } catch (SQLException e1) {
                        logger.error("Error on rollback command", e);
                    }
                }

                // This would unsubscribe from observer TODO ... but i don't like it
                throw new RuntimeException();
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.error("Error on closing connection", e);
                    }
                }
            }
        }
    }
}
