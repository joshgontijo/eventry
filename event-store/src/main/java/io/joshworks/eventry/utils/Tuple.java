package io.joshworks.eventry.utils;

public class Tuple<A, B> {

    public final A a;
    public final B b;

    private Tuple(A a, B b) {
        this.a = a;
        this.b = b;
    }

    public static <A, B> Tuple<A, B> of(A a, B b) {
        return new Tuple<>(a, b);
    }

    public A a() {
        return a;
    }

    public B b() {
        return b;
    }
}
