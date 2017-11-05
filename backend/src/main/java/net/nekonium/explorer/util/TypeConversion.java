package net.nekonium.explorer.util;

import java.math.BigInteger;

public class TypeConversion {

    public static byte[] bigIntegerToByteArrayLE(BigInteger bigInteger) {
        final byte[] be = bigInteger.toByteArray();
        final byte[] buffer = new byte[be.length];
        for (int i = buffer.length - 1; i >= 0; i--) {
            buffer[buffer.length - i - 1] = be[i];
        }

        return buffer;
    }
}
