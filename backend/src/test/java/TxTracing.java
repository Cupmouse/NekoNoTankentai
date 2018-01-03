import net.nekonium.explorer.web3jexpand.Web3jManager;
import net.nekonium.explorer.web3jexpand.response.DebugTraceTransaction;
import org.web3j.protocol.Service;
import org.web3j.protocol.core.Request;

import java.util.Arrays;

public class TxTracing {

    public static void main(String[] args) {
        final Web3jManager web3jManager = new Web3jManager();

        web3jManager.connect(Web3jManager.ConnectionType.RPC, "http://127.0.0.1:8293", false, 100);


        final Service service = web3jManager.getService();


        service.sendAsync(new Request("debug_traceTransaction",
                Arrays.asList("0x365a1d08acd6944f509b28d284cc74cbd856a400fd70219539bc818ed16438f4"),
                DebugTraceTransaction.class));

        EthBlock


    }

}
