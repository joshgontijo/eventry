package io.joshworks.fstore.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by josh on 3/4/17.
 */
public class EventStream extends LinkedList<Event> {

    private EventStream() {
    }

    private EventStream(Collection<? extends Event> c) {
        super(c);
    }

    public static EventStream empty() {
        return new EventStream(new LinkedList<>());
    }

    public static EventStream of(Collection<Event> events) {
        return new EventStream(events);
    }

    public static EventStream of(Event... events) {
        if (events == null || events.length == 0) {
            return empty();
        }
        return of(Arrays.asList(events));
    }
}
