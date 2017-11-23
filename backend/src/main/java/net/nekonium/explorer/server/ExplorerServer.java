package net.nekonium.explorer.server;

import net.nekonium.explorer.ExplorerBackend;
import net.nekonium.explorer.Web3jManager;
import net.nekonium.explorer.server.endpoint.BlockEndPoint;
import net.nekonium.explorer.server.endpoint.BlockNumberEndPoint;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.jsr356.server.ServerContainer;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;

import javax.servlet.ServletException;
import javax.websocket.DeploymentException;

public class ExplorerServer {

    private Server httpServer;
    private Web3jManager web3jManager;
    private static ExplorerBackend backend;

    public void start(ExplorerBackend backend) throws Exception {
        ExplorerServer.backend = backend;
        this.web3jManager = web3jManager;

        // Start websocket server

        this.httpServer = new Server();
        final ServerConnector connector = new ServerConnector(httpServer);

        connector.setHost("localhost");
        connector.setPort(8080);
        this.httpServer.addConnector(connector);


        final ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        this.httpServer.setHandler(handler);

        try {
            final ServerContainer container = WebSocketServerContainerInitializer.configureContext(handler);

            container.addEndpoint(BlockNumberEndPoint.class);
            container.addEndpoint(BlockEndPoint.class);
        } catch (ServletException | DeploymentException e) {
            e.printStackTrace();
        }

        this.httpServer.start();

        // Subscribe for a new block filter

        this.web3jManager.getWeb3j().blockObservable(true).subscribe(ethBlock -> {
            BlockNumberEndPoint.onNewBlock(ethBlock.getBlock());
            BlockEndPoint.onNewBlock(ethBlock.getBlock());
        });
    }

    public void stop() {
        try {
            this.httpServer.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ExplorerBackend getBackend() {
        return backend;
    }
}
