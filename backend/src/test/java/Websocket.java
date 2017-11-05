import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.ipc.WindowsIpcService;
import org.web3j.utils.Async;
import rx.Subscription;

import javax.servlet.ServletException;
import javax.websocket.*;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@ServerEndpoint("/block-stream")
public class Websocket {

    private static Queue<Session> sessionQueue = new ConcurrentLinkedQueue<>();

    @OnOpen
    public void onSessionOpen(Session session) {
        sessionQueue.offer(session);
    }

    @OnClose
    public void onSessionClose(Session session) {
        sessionQueue.remove(session);
    }

    public static void main(String[] args) {
        final Server server = new Server();
        final ServerConnector connector = new ServerConnector(server);
        connector.setPort(8080);
        connector.setHost("127.0.0.1");
        server.addConnector(connector);

        final ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        handler.setContextPath("/");
        server.setHandler(handler);

        try {
            final ServerContainer container = WebSocketServerContainerInitializer.configureContext(handler);

            container.addEndpoint(Websocket.class);

            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        final WindowsIpcService windowsIpcService = new WindowsIpcService("\\\\.\\pipe\\gnekonium.ipc");
        Web3j web3 = Web3j.build(windowsIpcService, 100, Async.defaultExecutorService());

        final Subscription subscription = web3.blockObservable(false).subscribe(blocks -> {
            System.out.println(blocks.getBlock().getNumber().toString());

            final Iterator<Session> iterator = sessionQueue.iterator();
            while (iterator.hasNext()) {
                iterator.next().getAsyncRemote().sendText(blocks.getBlock().getExtraData());
            }
        });

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("STOPPING");

        try {
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
