package io.joshworks.fstore.server;

import com.google.gson.Gson;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.snappy.sse.EventData;
import io.joshworks.snappy.sse.SseBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventBroadcast implements Runnable, Closeable {

    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<PollingSubscriber<Event>> pollers = new ArrayList<>();
    private final Gson gson = new Gson();

    private static final Logger logger = LoggerFactory.getLogger(EventBroadcast.class);
    private final long waitTime;

    public EventBroadcast(long waitTime) {
        this.waitTime = waitTime;
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {

                for (PollingSubscriber<Event> poller : pollers) {
                    Event event = poller.poll();
                    EventBody eventBody = EventBody.from(event);

                    String data = gson.toJson(eventBody);
                    String eventId = String.valueOf(event.position());
                    String stream = event.stream();

                    EventData eventData = new EventData(data, eventId, stream);
                    SseBroadcaster.broadcast(eventData, stream);



                }



            }

        } catch (InterruptedException e) {
           //TODO handle error
            e.printStackTrace();
        }
    }


    @Override
    public void close() throws IOException {
        closed.set(true);
    }
}
