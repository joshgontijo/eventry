package io.joshworks.fstore.server;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.es.EventStore;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.snappy.sse.SseBroadcaster;
import io.undertow.server.handlers.sse.ServerSentEventConnection;

import java.util.HashSet;
import java.util.Set;

public class SubscriptionEndpoint {

    public static final String PATH_PARAM_STREAM = "streamId";

    private final EventStore store;

    private Set<PollingSubscriber<Event>> pollers = new HashSet<>();

    public SubscriptionEndpoint(EventStore store) {
        this.store = store;
    }

    public void newPushHandler(ServerSentEventConnection connection, String lastEventId) {
        String streamId = connection.getParameter(PATH_PARAM_STREAM);

        int version = lastEventId == null || lastEventId.isEmpty() ? 0 : Integer.valueOf(lastEventId);
        PollingSubscriber<Event> poller = store.poller(streamId, version);
        pollers.add(poller);
        //TODO implement me
        Thread thread = new Thread(new EventBroadcast(poller));
        thread.start();

        connection.addCloseTask(conn -> {
            IOUtils.closeQuietly(poller);
            pollers.remove(poller);
        });

        SseBroadcaster.addToGroup(streamId, connection);
    }

//    private long process(ServerSentEventConnection connection, String streamId, String lastEventId) {
//        int lastVersion = Integer.parseInt(lastEventId);
//
//
//        return store.fromStream(streamId, lastVersion + 1).peek(event -> {
//            String payload = EventBody.from(event).toJson();
//            String version = String.valueOf(event.version());
//            connection.send(payload, streamId, version, null);
//        }).mapToLong(Event::position).reduce((first, second) -> second).orElse(0L);
//
//
//    }


}
