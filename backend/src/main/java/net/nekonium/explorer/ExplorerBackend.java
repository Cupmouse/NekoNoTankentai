package net.nekonium.explorer;

import net.nekonium.explorer.server.endpoint.BlockNumberEndPoint;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.jsr356.server.ServerContainer;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.ipc.WindowsIpcService;

import javax.servlet.ServletException;
import javax.websocket.DeploymentException;
import java.sql.SQLException;

public class ExplorerBackend {

    private Web3jManager web3jManager;
    private BlockchainConverter converter;
    private Thread converterThread;

    public void start() throws SQLException {
        this.web3jManager = new Web3jManager();
        // FIXME On IPC connection, quick mass data fetching like a catch up fetching cause an data corruption and stop an observer thread. maybe a web3j issue
        web3jManager.connect(Web3jManager.ConnectionType.RPC, "http://127.0.0.1:8293", false, 100);

        this.web3jManager.getWeb3j().blockObservable(false).subscribe(ethBlock -> {
            BlockNumberEndPoint.onNewBlock(ethBlock.getBlock());
        });

        // Start blockchain-to-database conversion
        this.converter = new BlockchainConverter(web3jManager);
        this.converter.init();

        this.converterThread = new Thread(converter);
        this.converterThread.start();


        // Start websocket server
        final Server server = new Server();
        final ServerConnector connector = new ServerConnector(server);

        connector.setHost("localhost");
        connector.setPort(8080);
        server.addConnector(connector);


        final ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        server.setHandler(handler);

        try {
            final ServerContainer container = WebSocketServerContainerInitializer.configureContext(handler);

            container.addEndpoint(BlockNumberEndPoint.class);
        } catch (ServletException | DeploymentException e) {
            e.printStackTrace();
        }

        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                server.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
