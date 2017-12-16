package net.nekonium.explorer.util;

import java.util.Objects;

public class NonNullPair<A, B> {

    private final A a;
    private final B b;

    public NonNullPair(A a, B b) {
        if (a == null) {
            throw new NullPointerException("a is null");
        }
        if (b == null) {
            throw new NullPointerException("b is null");
        }
        this.a = a;
        this.b = b;
    }

    public A getA() {
        return a;
    }

    public B getB() {
        return b;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NonNullPair<?, ?> nonNullPair = (NonNullPair<?, ?>) o;
        return Objects.equals(a, nonNullPair.a) &&
                Objects.equals(b, nonNullPair.b);
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b);
    }
}
