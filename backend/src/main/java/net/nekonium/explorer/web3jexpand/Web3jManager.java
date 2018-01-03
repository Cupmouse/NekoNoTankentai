package net.nekonium.explorer.web3jexpand;

import org.web3j.protocol.Service;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;
import org.web3j.protocol.ipc.UnixIpcService;
import org.web3j.protocol.ipc.WindowsIpcService;
import org.web3j.utils.Async;

public class Web3jManager {

    private Web3j web3j;
    private Service service;

    private ConnectionType determineIPC() {
        final String osName = System.getProperty("os.name");

        if (osName.toLowerCase().startsWith("windows")) {
            return ConnectionType.IPC_WINDOWS;
        } else {
            return ConnectionType.IPC_UNIX;
        }
    }

    public void connectIPC(String path, long pollingInterval) {
        connect(determineIPC(), path, false, pollingInterval);
    }

    public void connect(ConnectionType type, String path, boolean includeRawResponse, long pollingInterval) {

        Service service = null;

        switch (type) {
            case IPC_UNIX:
                service = new UnixIpcService(path, includeRawResponse);
                break;
            case IPC_WINDOWS:
                service = new WindowsIpcService(path, includeRawResponse);
                break;
            case RPC:
                service = new HttpService(path, includeRawResponse);
                break;
        }

        this.web3j = Web3j.build(service, pollingInterval, Async.defaultExecutorService());
        this.service = service;
    }

    public Web3j getWeb3j() {
        if (web3j == null) {
            throw new IllegalStateException("Web3jManager is not initialized!");
        }

        return web3j;
    }

    public Service getService() {
        return service;
    }

    public enum ConnectionType {
        IPC_UNIX, IPC_WINDOWS, RPC
    }
}
