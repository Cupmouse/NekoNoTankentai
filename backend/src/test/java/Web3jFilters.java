import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;
import org.web3j.protocol.ipc.WindowsIpcService;
import org.web3j.utils.Async;
import rx.Subscription;

public class Web3jFilters {

    public static void main(String[] args) {
        final WindowsIpcService windowsIpcService = new WindowsIpcService("\\\\.\\pipe\\gnekonium.ipc");
        Web3j web3 = Web3j.build(windowsIpcService, 100, Async.defaultExecutorService());

        final Subscription subscription = web3.blockObservable(false).subscribe(blocks -> System.out.println(blocks.getBlock().getNumber()));

    }
}
