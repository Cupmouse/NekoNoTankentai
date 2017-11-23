package net.nekonium.explorer.server;

import net.nekonium.explorer.ExplorerBackend;
import net.nekonium.explorer.server.endpoint.BlockEndPoint;
import net.nekonium.explorer.server.endpoint.BlockNumberEndPoint;
import net.nekonium.explorer.server.endpoint.RequestEndPoint;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.jsr356.server.ServerContainer;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.websocket.DeploymentException;

public class ExplorerServer {

    private static ExplorerServer instance;

    private Server httpServer;
    private org.slf4j.Logger logger;
    private ExplorerBackend backend;

    public ExplorerServer(ExplorerBackend backend) {
        this.backend = backend;
        this.logger = LoggerFactory.getLogger("Server");    // Get your logger (TM)
        ExplorerServer.instance = this;
    }

    public static ExplorerServer getInstance() {
        return instance;
    }

    public void start() throws Exception {
        /* Start websocket server (I don't know what this do) */

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
            container.addEndpoint(RequestEndPoint.class);
        } catch (ServletException | DeploymentException e) {
            e.printStackTrace();
        }

        RequestEndPoint.initQueryExecutor(); // Initialize RequestEndPoint's query executor before starting a server

        this.httpServer.start();

        /* Subscribe for a new block filter */

        this.backend.getWeb3jManager().getWeb3j().blockObservable(true).subscribe(ethBlock -> {
            BlockNumberEndPoint.onNewBlock(ethBlock.getBlock());
            BlockEndPoint.onNewBlock(ethBlock.getBlock());
        });
    }

    public void stop() {
        RequestEndPoint.shutdownQueryExecutorAndWait(); // Shutdown RequestEndPoint's query executor before shutting down an actual server. All requests coming in after the executor shutdown will be ignored

        try {
            this.httpServer.stop(); // Stop the whole backend server
            this.httpServer.join(); // fixme if exception occurred on stop, this wont be called
        } catch (Exception e) {
            this.logger.error("An error occurred when stopping a server", e);
        }
    }

    public org.slf4j.Logger getLogger() {
        return logger;
    }

    public ExplorerBackend getBackend() {
        return backend;
    }
}
