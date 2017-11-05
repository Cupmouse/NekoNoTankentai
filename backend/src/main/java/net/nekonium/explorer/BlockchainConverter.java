package net.nekonium.explorer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.nekonium.explorer.util.TypeConversion;
import org.json.JSONArray;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Transaction;
import org.web3j.protocol.http.HttpService;
import rx.Observable;

import java.math.BigInteger;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

import static java.sql.Statement.RETURN_GENERATED_KEYS;

public class BlockchainConverter implements Runnable {

    private final Web3jManager web3jManager;
    private HikariDataSource dataSource;

    public BlockchainConverter(Web3jManager web3jManager) {
        this.web3jManager = web3jManager;
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


            // Fetching all block data from catchupFetchStart to the latest block
            System.out.println("Catching up fetching is starting from block #" + catchupFetchStart);

            final Observable<EthBlock> observable = this.web3jManager.getWeb3j().catchUpToLatestBlockObservable(DefaultBlockParameter.valueOf(BigInteger.valueOf(catchupFetchStart)), true);
            observable.subscribe(ethBlock -> {
                final EthBlock.Block block = ethBlock.getBlock();

                System.out.println("Got new block #" + block.getNumber());

                final JSONArray jsonArrayTransactions = new JSONArray();
                block.getTransactions().stream()
                        .map(transactionResult -> ((EthBlock.TransactionObject) transactionResult).get().getHash())
                        .forEach(jsonArrayTransactions::put);
                final String jsonTransactionHashes = jsonArrayTransactions.toString();

                final JSONArray jsonArrayUncles = new JSONArray();
                block.getUncles().forEach(jsonArrayUncles::put);
                final String jsonUncles = jsonArrayUncles.toString();

                try {

                    // First, transactions

                    for (EthBlock.TransactionResult result : block.getTransactions()) {
                        final Transaction transaction = (Transaction) result.get();

                        final PreparedStatement prpstmt = connection.prepareStatement(
                                "INSERT INTO transactions VALUES " +
                                        "(?, UNHEX(?), ?, ?, UNHEX(?), UNHEX(?), ?, UNHEX(?), UNHEX(?), UNHEX(?), " +
                                        "?, ?, ?)"
                                , RETURN_GENERATED_KEYS);
                        prpstmt.setString(1, transaction.getBlockNumber().toString());
                        prpstmt.setString(2, transaction.getFrom().substring(2));
                        prpstmt.setString(3, transaction.getGas().toString());
                        prpstmt.setBytes(4, transaction.getGasPrice().toByteArray());
                        prpstmt.setString(5, transaction.getHash().substring(2));
                        prpstmt.setString(6, transaction.getInput().substring(2));
                        prpstmt.setString(7, transaction.getNonce().toString());
                        prpstmt.setString(8, transaction.getR().substring(2));
                        prpstmt.setString(9, transaction.getS().substring(2));
                        prpstmt.setString(10, transaction.getTo() == null ? null : transaction.getTo().substring(2));
                        prpstmt.setString(11, transaction.getTransactionIndex().toString());
                        prpstmt.setInt(12, transaction.getV());
                        prpstmt.setBytes(13, transaction.getValue().toByteArray());
                        prpstmt.executeUpdate();
                    }

                    // Second, block
                    final PreparedStatement prpstmt = connection.prepareStatement(
                            "INSERT INTO blocks VALUES " +
                                    "(?, UNHEX(?), ?, ?, UNHEX(?), UNHEX(?), UNHEX(?), UNHEX(?), ?, ?, " +
                                    "UNHEX(?), UNHEX(?), UNHEX(?), ?, UNHEX(?), FROM_UNIXTIME(?), ?, ?, UNHEX(?), ?)"
                            , RETURN_GENERATED_KEYS);

                    // FIXME Difficulty integer type on mysql and java miss-match, using java long will overflow on large number
                    // TODO Every integer number on go-nekonium is non-fixed integer, it will overflow on both mysql and java in future (distant future)
                    prpstmt.setString(1, block.getDifficulty().toString());
                    prpstmt.setString(2, block.getExtraData().substring(2));
                    prpstmt.setString(3, block.getGasLimit().toString());
                    prpstmt.setString(4, block.getGasUsed().toString());
                    prpstmt.setString(5, block.getHash().substring(2));
                    prpstmt.setString(6, block.getLogsBloom().substring(2));
                    prpstmt.setString(7, block.getMiner().substring(2));
                    prpstmt.setString(8, block.getMixHash().substring(2));
                    prpstmt.setString(9, block.getNonce().toString());
                    prpstmt.setString(10, block.getNumber().toString());
                    prpstmt.setString(11, block.getParentHash().substring(2));
                    prpstmt.setString(12, block.getReceiptsRoot().substring(2));
                    prpstmt.setString(13, block.getSha3Uncles().substring(2));
                    prpstmt.setString(14, block.getSize().toString());
                    prpstmt.setString(15, block.getStateRoot().substring(2));
                    prpstmt.setString(16, block.getTimestamp().toString());
                    prpstmt.setBytes(17, block.getTotalDifficulty().toByteArray());
                    prpstmt.setString(18, jsonTransactionHashes);
                    prpstmt.setString(19, block.getTransactionsRoot().substring(2));
                    prpstmt.setString(20, jsonUncles);

                    prpstmt.executeUpdate();
                    final ResultSet r = prpstmt.getGeneratedKeys();
                    if (r.next()) {
                        r.getInt(1)
                    }
                    prpstmt.close();

                    // And finally, commit it
                    connection.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    try {
                        connection.rollback();
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    }
                    throw new RuntimeException(e);
                }

            });
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
