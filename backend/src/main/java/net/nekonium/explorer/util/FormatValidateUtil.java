package net.nekonium.explorer.util;

import java.util.regex.Pattern;

public class FormatValidator {

    public static final Pattern HEX_32BYTES = Pattern.compile("^0x[A-Fa-f0-9]{64}$");

    private FormatValidator() {
    }

    public static boolean isValidTransactionHash(String hash) {
        return (hash.length() == 2 + 64) && HEX_32BYTES.matcher(hash).matches();
    }

    public static boolean isValidBlockHash(String hash) {
        return (hash.length() == 2 + 64) && HEX_32BYTES.matcher(hash).matches();
    }
}
