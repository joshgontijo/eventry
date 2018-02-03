package io.joshworks.fstore.log;

import java.util.Iterator;

public interface Reader<T> extends Iterable<T>, Iterator<T> {

   long position();

}
