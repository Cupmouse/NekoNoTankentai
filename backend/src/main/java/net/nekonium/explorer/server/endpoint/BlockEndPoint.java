package net.nekonium.explorer.server.endpoint;

import net.nekonium.explorer.util.SessionPair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Transaction;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Emits all block changes real-time with complete block information including transaction objects
 */
@ServerEndpoint("/block")
public class BlockEndPoint {

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
    public void onMessage(Session session, String message) throws IOException {
        // No message from client allowed!
        session.close();
    }

    public static void onNewBlock(EthBlock.Block block) {
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("number", block.getNumber());
        jsonObject.put("hash", block.getHash());
        jsonObject.put("timestamp", block.getTimestamp());
        jsonObject.put("miner", block.getMiner());
        jsonObject.put("nonce", block.getNonce());
        jsonObject.put("gas_used", block.getGasUsed());
        jsonObject.put("gas_limit", block.getGasUsed());

        final JSONArray jsonArrayUncles = new JSONArray();
        for (String uncleHash : block.getUncles()) {
            jsonArrayUncles.put(uncleHash);
        }
        jsonObject.put("uncles", jsonArrayUncles);

        final JSONArray jsonArrayTransactions = new JSONArray();
        for (EthBlock.TransactionResult transactionResult : block.getTransactions()) {
            final Transaction transaction = ((EthBlock.TransactionObject) transactionResult).get();

            final JSONObject jsonObjectTransaction = new JSONObject();
            jsonObjectTransaction.put("hash", transaction.getHash());
            jsonObjectTransaction.put("from", transaction.getFrom());
            jsonObjectTransaction.put("to", transaction.getTo());
            jsonObjectTransaction.put("value", transaction.getValue());
            jsonObjectTransaction.put("gas", transaction.getGas());
            jsonObjectTransaction.put("nonce", transaction.getNonce());
            jsonObjectTransaction.put("data", transaction.getInput());
            jsonArrayTransactions.put(jsonObjectTransaction);
        }

        jsonObject.put("transactions", jsonArrayTransactions);

        final String message = jsonObject.toString();

        for (Session session : sessionQueue) {
            session.getAsyncRemote().sendText(message);
        }
    }
}
