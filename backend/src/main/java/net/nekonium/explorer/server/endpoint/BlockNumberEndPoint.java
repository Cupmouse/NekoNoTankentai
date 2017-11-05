package net.nekonium.explorer.server.endpoint;

import org.web3j.protocol.core.methods.response.EthBlock;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 最新のブロック高さを送信するエンドポイントです。
 * 高さは同じでもハッシュ値が異なる場合は再送されます
 */
@ServerEndpoint("/block-number")
public class BlockNumberEndPoint {

    private static Queue<Session> sessionQueue = new ConcurrentLinkedQueue<>();

    @OnOpen
    public void onSessionOpen(Session session) {
        sessionQueue.add(session);
    }

    @OnClose
    public void onSessionClose(Session session) {
        sessionQueue.remove(session);
    }

    @OnMessage
    public void onMessage(String s, Session session) throws IOException {
        // No message from client allowed!
        session.close();
    }

    public static void onNewBlock(EthBlock.Block block) {
        final String message = block.getNumber().toString();

        // TODO 多分非同期なのでエラーが起こる
        for (Session session : sessionQueue) {
            session.getAsyncRemote().sendText(message);
        }
    }
}
