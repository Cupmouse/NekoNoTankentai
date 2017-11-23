package net.nekonium.explorer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.nekonium.explorer.util.IllegalBlockchainStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Transaction;
import org.web3j.protocol.core.methods.response.TransactionReceipt;
import rx.Subscription;
import rx.functions.Action1;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static java.sql.Statement.RETURN_GENERATED_KEYS;

public class BlockchainConverter implements Runnable {
//
//    // transfer(address,uint256) => a9059cbb2ab09eb219583f4a59a5d0623ade346d962bcd4e46b11da047c9049b
//    public static String KECCAK_ERC20_TRANSFER = "0xa9059cbb";
//    public static String KECCAK_ERC20_

    private final Web3jManager web3jManager;
    private final DatabaseManager databaseManager;
    private final Logger logger;

    private Subscription blockSub;
    private boolean stop;

    public BlockchainConverter(Web3jManager web3jManager, DatabaseManager databaseManager) {
        this.web3jManager = web3jManager;
        this.databaseManager = databaseManager;
        this.logger = LoggerFactory.getLogger("Converter");
    }

    @Override
    public void run() {

        // Let's check the block number where to start fetching from
        // Using BigInteger for avoiding overflow, as go-nekonium uses an arbitrary precision integer for block number
        BigInteger catchupStart;
        Connection connection = null;

        try {
            connection = databaseManager.getConnection();

            // Set auto-commit off
            connection.setAutoCommit(false);

            logger.info("Getting the most recent fetched block number from the database...");

            final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery("SELECT number FROM blocks ORDER BY number DESC LIMIT 1");

            if (resultSet.next()) {
                catchupStart = new BigInteger(resultSet.getString(1)).add(BigInteger.ONE);
            } else {
                // Found no data on the database, fetching blockchain data from the block 0
                catchupStart = BigInteger.ZERO;
            }

            statement.close();
        } catch (SQLException e) {
            this.logger.error("A database error occurred during getting the latest block number, Stopping converter", e);

            // Stop converter
            return;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    this.logger.error("An error occurred when closing a connection to the database");
                }
            }
        }

        // Get the current tallest block's number from nekonium node (for catchup fetch)

        this.logger.info("Getting the latest block number from the nekonium node");

        final BigInteger nodeBlockNumber;

        try {
            nodeBlockNumber = this.web3jManager.getWeb3j().ethBlockNumber().send().getBlockNumber();
        } catch (IOException e) {
            this.logger.error("An error occurred when getting executing eth_blockNumber method call to the nekonium node");

            // Stop converter
            this.stop = true;
            return;
        }

        if (nodeBlockNumber.compareTo(catchupStart) > 0) {
            // Latest block number is grater than the number on the database
            // Catchup fetch is needed

            // Fetching all block data from catchupStart to the latest block, this is catchup fetch
            this.logger.info("A catchup fetch is starting from the block #{} to #{}", catchupStart.toString(), nodeBlockNumber.toString());

            this.blockSub = this.web3jManager.getWeb3j()
                    .replayBlocksObservable(DefaultBlockParameter.valueOf(catchupStart), DefaultBlockParameter.valueOf(nodeBlockNumber), true)
                    // This happens on error and stops converter
                    .doOnError(throwable -> stop = true)
                    .subscribe(new CatchupSubscriber(nodeBlockNumber));

            this.logger.info("Catching up...");

            // Wait until fetching is over
            while (!this.blockSub.isUnsubscribed()) {
                // If subscription unsubscribed on catchUpToLatestBlockObservable then fetching all of block data is done
                // FIXME I hate this, is there any more efficient way to get notified if block fetch is done???

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (stop) {
                // Look like there was an error in the subscriber's thread
                // Stop the whole converter
                this.logger.warn("Catchup fetch failed. Stopping converter...");
                return;
            }
        }

        this.logger.info("Catchup fetch completed. Initiating a real-time fetch...");

        // Real-time fetch, this continues through a block explorer is operational, starts from catchup fetch end + 1
        this.blockSub = web3jManager.getWeb3j()
                .catchUpToLatestAndSubscribeToNewBlocksObservable(DefaultBlockParameter.valueOf(nodeBlockNumber.add(BigInteger.ONE)), true)
                .doOnUnsubscribe(() -> logger.debug("REAL-TIME FETCH UNSUBSCRIBED"))
                .subscribe(new NormalSubscriber());

        this.logger.info("Real-time fetch started");

        // Waiting
        while (!this.blockSub.isUnsubscribed()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Commit all of the data involving {@code block}, including uncleBlocks, which sends sync request for nekonium node
     *
     * @param connection
     * @param block
     * @throws SQLException
     * @throws IOException
     */
    private void commitBlock(Connection connection, EthBlock.Block block) throws SQLException, IOException, IllegalBlockchainStateException {
        int n;

        try {
            // Get all of uncles blocks in advance if it exist

            final List<String> uncleHashes = block.getUncles();
            final EthBlock.Block[] uncleBlocks = new EthBlock.Block[uncleHashes.size()];

            if (uncleHashes.size() > 0) {
                for (int i = 0; i < uncleHashes.size(); i++) {
                    // Get by block hash MAYBE able to retrieve forked blocks, otherwise this function call can fail
                    final EthBlock ethBlock = web3jManager.getWeb3j().ethGetUncleByBlockHashAndIndex(block.getHash(), BigInteger.valueOf(i)).send();
                    uncleBlocks[i] = ethBlock.getBlock();
                }
            }

            // Get all of transaction receipt too
            final List<EthBlock.TransactionResult> transactionResults = block.getTransactions();
            final Transaction[] transactions = new Transaction[transactionResults.size()];
            final TransactionReceipt[] transactionReceipts = new TransactionReceipt[transactionResults.size()];

            for (int i = 0; i < transactions.length; i++) {
                transactions[i] = (Transaction) transactionResults.get(i).get();

                final Optional<TransactionReceipt> transactionReceiptOptional = web3jManager.getWeb3j().ethGetTransactionReceipt(transactions[i].getHash()).send().getTransactionReceipt();

                if (!transactionReceiptOptional.isPresent()) {
                    throw new IllegalBlockchainStateException("An transaction receipt of a transaction included in the requested block not exist on the blockchain");
                }

                transactionReceipts[i] = transactionReceiptOptional.get();
            }


            // First, insert the block

            final PreparedStatement prpstmt = connection.prepareStatement(
                    "INSERT INTO blocks VALUES " +
                            "(NULL, ?, UNHEX(?), UNHEX(?), FROM_UNIXTIME(?), UNHEX(?), ?, ?, ?, UNHEX(?), ?, " +
                            "UNHEX(?), ?)", RETURN_GENERATED_KEYS);

            // TODO Every integer number on go-nekonium is arbitrary integer, it will overflow on mysql in future (distant future)
            n = 0;
            prpstmt.setString(++n, block.getNumber().toString());
            prpstmt.setString(++n, block.getHash().substring(2));
            prpstmt.setString(++n, block.getParentHash().substring(2));
            prpstmt.setString(++n, block.getTimestamp().toString());
            prpstmt.setString(++n, block.getMiner().substring(2));
            prpstmt.setString(++n, block.getDifficulty().toString());
            prpstmt.setString(++n, block.getGasLimit().toString());
            prpstmt.setString(++n, block.getGasUsed().toString());
            prpstmt.setString(++n, block.getExtraData().substring(2));
            prpstmt.setString(++n, block.getNonce().toString());
            prpstmt.setString(++n, block.getSha3Uncles().substring(2));
            prpstmt.setString(++n, block.getSize().toString());

            prpstmt.executeUpdate();
            // Get AUTO_INCLEMENT value the same time as the update execution
            final ResultSet generatedKeys = prpstmt.getGeneratedKeys();
            if (!generatedKeys.next()) {
                // If AUTO_INCLEMENT was not set, it should consider to be an error
                throw new RuntimeException();
            }
            final long blockInternalId = generatedKeys.getLong(1);
            prpstmt.close();

            // Second, insert uncle blocks ... usualy just A block. easy job

            for (int i = 0; i < uncleBlocks.length; i++) {
                final EthBlock.Block uncleBlock = uncleBlocks[i];

                final PreparedStatement prpstmt2 = connection.prepareStatement(
                        "INSERT INTO uncle_blocks VALUES " +
                                "(NULL, ?, ?, ?, UNHEX(?), UNHEX(?), FROM_UNIXTIME(?), UNHEX(?), ?, ?, ?, UNHEX(?), " +
                                "?, UNHEX(?), ?)");


                n = 0;
                prpstmt2.setString(++n, uncleBlock.getNumber().toString());           // This uncle block's block number
                prpstmt2.setLong(++n, blockInternalId);                               // Block internal id (NOT always same as block number) that this uncle block is included
                prpstmt2.setInt(++n, i);                                              // i is uncle index. gnekonium allows only 2 uncle blocks in a single block
                prpstmt2.setString(++n, uncleBlock.getHash().substring(2));
                prpstmt2.setString(++n, uncleBlock.getParentHash().substring(2));
                prpstmt2.setString(++n, uncleBlock.getTimestamp().toString());
                prpstmt2.setString(++n, uncleBlock.getMiner().substring(2));
                prpstmt2.setString(++n, uncleBlock.getDifficulty().toString());
                prpstmt2.setString(++n, uncleBlock.getGasLimit().toString());
                prpstmt2.setString(++n, uncleBlock.getGasUsed().toString());
                prpstmt2.setString(++n, uncleBlock.getExtraData().substring(2));
                prpstmt2.setString(++n, uncleBlock.getNonce().toString());
                prpstmt2.setString(++n, uncleBlock.getSha3Uncles().substring(2));
                prpstmt2.setString(++n, uncleBlock.getSize().toString());

                prpstmt2.executeUpdate();
                prpstmt2.close();
            }


            // Third, insert transactions

            for (int i = 0; i < transactions.length; i++) {
                final Transaction transaction = transactions[i];
                final TransactionReceipt transactionReceipt = transactionReceipts[i];

                final PreparedStatement prpstmt3 = connection.prepareStatement(
                        "INSERT INTO transactions VALUES " +
                                "(NULL, ?, ?, UNHEX(?), UNHEX(?), UNHEX(?), UNHEX(?), ?, ?, ?, ?, ?, UNHEX(?))");
                n = 0;
                prpstmt3.setLong(++n, blockInternalId);
                prpstmt3.setString(++n, transaction.getTransactionIndex().toString());
                prpstmt3.setString(++n, transaction.getHash().substring(2));
                prpstmt3.setString(++n, transaction.getFrom().substring(2));
                prpstmt3.setString(++n, transaction.getTo() == null ? null : transaction.getTo().substring(2));
                prpstmt3.setString(++n, transactionReceipt.getContractAddress() == null ? null : transactionReceipt.getContractAddress().substring(2));
                prpstmt3.setBytes(++n, transaction.getValue().toByteArray());
                prpstmt3.setString(++n, transaction.getGas().toString());
                prpstmt3.setString(++n, transactionReceipt.getGasUsed().toString());
                prpstmt3.setBytes(++n, transaction.getGasPrice().toByteArray());
                prpstmt3.setString(++n, transaction.getNonce().toString());
                prpstmt3.setString(++n, transaction.getInput().substring(2));

                prpstmt3.executeUpdate();
                prpstmt3.close();

                if (!transaction.getValue().equals(BigInteger.ZERO)) {
                    // TODO Nuko sending transaction, calculate address balance changes


                }

            }

            // And finally, commit it
            connection.commit();
        } catch (IOException | SQLException | RuntimeException e) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                this.logger.error("An error occurred when performing a rollback on the database", e);
            }

            // Throw back to where method called
            throw e;
        }
    }

    private class NormalSubscriber implements Action1<EthBlock> {

        @Override
        public void call(EthBlock ethBlock) {
            logger.debug("NEW BLOCK {}", ethBlock.getBlock().getNumber().toString());

            Connection connection = null;

            try {
                connection = databaseManager.getConnection();
                commitBlock(connection, ethBlock.getBlock());
            } catch (SQLException | IOException e) {
                logger.error("An error occurred during committing a new block", e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.error("An error occurred during closing a database connection", e);
                    }
                }
            }

        }
    }

    private class CatchupSubscriber implements Action1<EthBlock> {

        private final BigInteger BI_100 = BigInteger.valueOf(100);

        private final BigInteger catchupGoal;
        private BigInteger blockCount = BigInteger.ZERO;
        private LinkedList<Long> times = new LinkedList<>();

        public CatchupSubscriber(BigInteger catchupGoal) {
            this.catchupGoal = catchupGoal;
        }

        @Override
        public void call(EthBlock ethBlock) {
            Connection connection = null;

            try {
                // Get a connection from pool
                connection = databaseManager.getConnection();

                // Get a block response
                final EthBlock.Block block = ethBlock.getBlock();
                final BigInteger blockNumber = block.getNumber();

                commitBlock(connection, block);
                // Incrementing and set new value for blockCount, be careful that BigInteger is IMMUTABLE.
                this.blockCount = blockCount.add(BigInteger.ONE);

                if (blockCount.mod(BI_100).equals(BigInteger.ZERO)) {
                    addSample(System.currentTimeMillis());

                    // Show progress each time fetching 100 blocks
                    final BigDecimal progressp = new BigDecimal(blockNumber.multiply(BI_100))
                            .divide(new BigDecimal(catchupGoal), 2, RoundingMode.HALF_UP);

                    logger.info("Catching up... Fetched {} blocks, the latest is #{}/{} est finish in {}minutes ({}%)",
                            blockCount, blockNumber.toString(),
                            catchupGoal,
                            calMinutes(blockNumber),
                            progressp.toString());
                }
            } catch (Exception e) {
                logger.error("Database issue! Stopping catchup fetch", e);

                stop = true;

                // This would unsubscribe from observer
                blockSub.unsubscribe();
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.error("An error occurred during closing connection", e);
                    }
                }
            }
        }

        private void addSample(long timeMillis) {
            this.times.addFirst(System.currentTimeMillis());

            if (times.size() > 300) {
                this.times.removeLast();
            }
        }

        private String calMinutes(BigInteger blockNumber) {
            if (times.size() == 1) {
                // If no sampled time is available, just return "?"
                return "?";
            }

            long total = 0;

            // The last entry is not comparable
            for (int i = 0; i < times.size() - 1; i++) {
                total += times.get(i) - times.get(i + 1);
            }

            // Get average of time take to perform an one operation
            // One operation is 100 blocks insert

            return new BigDecimal(total)
                    .multiply(new BigDecimal(catchupGoal.subtract(blockNumber)))
                    .divide(BigDecimal.valueOf(1000L * 60 * 100 * times.size()), 2, RoundingMode.HALF_UP).toString();
        }
    }

    public void stop() {
        // Unsubscribe block filter
        this.blockSub.unsubscribe();
    }
}
