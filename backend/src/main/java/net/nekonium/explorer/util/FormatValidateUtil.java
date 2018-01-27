package net.nekonium.explorer.util;

import java.util.regex.Pattern;

public class FormatValidateUtil {

    public static final Pattern HEX_32BYTES = Pattern.compile("^0x[A-Fa-f0-9]{64}$");
    public static final Pattern HEX_20BYTES = Pattern.compile("^0x[A-Fa-f0-9]{40}$");

    private FormatValidateUtil() {
    }

    public static boolean isValidTransactionHash(String hash) {
        return (hash.length() == 2 + 64) && HEX_32BYTES.matcher(hash).matches();
    }

    public static boolean isValidBlockHash(String hash) {
        return (hash.length() == 2 + 64) && HEX_32BYTES.matcher(hash).matches();
    }

    public static boolean isValidAddressHash(String hash) {
        return (hash.length() == 2 + 40) && HEX_20BYTES.matcher(hash).matches();
    }
}
