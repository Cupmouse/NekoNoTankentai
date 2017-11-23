package net.nekonium.explorer.util;

import java.math.BigInteger;
import java.time.format.DateTimeFormatter;

public class TypeConversion {

    public static final DateTimeFormatter MYSQL_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static byte[] bigIntegerToByteArrayLE(BigInteger bigInteger) {
        final byte[] be = bigInteger.toByteArray();
        final byte[] buffer = new byte[be.length];
        for (int i = buffer.length - 1; i >= 0; i--) {
            buffer[buffer.length - i - 1] = be[i];
        }

        return buffer;
    }
}
