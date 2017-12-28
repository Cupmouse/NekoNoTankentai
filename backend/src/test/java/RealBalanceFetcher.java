import net.nekonium.explorer.Web3jManager;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;

import java.io.IOException;
import java.math.BigInteger;

public class RealBalanceFetcher {

    public static void main(String[] args) {
        Web3jManager web3jManager = new Web3jManager();

        web3jManager.connect(Web3jManager.ConnectionType.RPC, "http://127.0.0.1:8293", false, 100);

        web3jManager.getWeb3j().blockObservable(false).subscribe(ethBlock -> {
            try {
                BigInteger number = ethBlock.getBlock().getNumber();
                System.out.println("Block #" + number + " : " +  web3jManager.getWeb3j().ethGetBalance("0x2b14a494921680b54fd234d4d20a430d6a03b5c1", DefaultBlockParameter.valueOf(number)).send().getBalance());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

}
