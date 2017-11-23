package net.nekonium.explorer;

import net.nekonium.explorer.server.ExplorerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ExplorerBackend {

    private Logger logger;
    private Web3jManager web3jManager;
    private BlockchainConverter converter;
    private Thread converterThread;
    private ExplorerServer webSocketServer;
    private DatabaseManager databaseManager;

    public void start() throws Exception {
        this.logger = LoggerFactory.getLogger("ExplorerBackend");

        this.web3jManager = new Web3jManager();

        // Connect to the nekonium-node (usually a go-nekonium client)
        // FIXME On IPC connection, quick mass data fetching like a catch up fetching cause an data corruption and stop an observer thread. maybe a web3j issue
        this.web3jManager.connect(Web3jManager.ConnectionType.RPC, "http://127.0.0.1:8293", false, 100);

        // Initialize database
        this.databaseManager = new DatabaseManager();
        this.databaseManager.init();

        // Start blockchain-to-database conversion
        this.converter = new BlockchainConverter(web3jManager, databaseManager);

        this.converterThread = new Thread(converter);
        this.converterThread.start();

        // Start websocket server

        this.webSocketServer = new ExplorerServer();
        this.webSocketServer.start(this);

        // TODO needs to watch if those nasty multi threads are causing error and stopping

        // Handling console command
        final BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            final String line = stdin.readLine();

            System.out.println("line was : " + line);

            if (line.equalsIgnoreCase("stop")) {
                break;
            } else {
                System.out.print("Cmd>");
            }
        }

        // Initiating stop order
        this.webSocketServer.stop();
        this.converter.stop();
        // Wait for converter thread to stop its job
        this.converterThread.join();
    }

    public Web3jManager getWeb3jManager() {
        return web3jManager;
    }

    public DatabaseManager getDatabaseManager() {
        return databaseManager;
    }
}
