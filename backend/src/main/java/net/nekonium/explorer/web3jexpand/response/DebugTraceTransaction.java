package net.nekonium.explorer.web3jexpand.response;

import org.web3j.protocol.core.Response;

import java.math.BigInteger;
import java.util.List;

public class DebugTraceTransaction extends Response<DebugTraceTransaction.TraceTransaction> {

    private BigInteger gas;
    private String returnValue;
    private List<StructLog> structLogs;

    public static class TraceTransaction {

    }

    private static class StructLog {
        private int depth;
        private String error;
        private long gas;
        private long gasCost;
        private List<String> memory;
        private String op;
        private long pc;
        private List<String> stack;
    }
}
