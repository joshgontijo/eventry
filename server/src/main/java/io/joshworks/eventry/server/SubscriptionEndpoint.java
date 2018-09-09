package io.joshworks.eventry.server;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.snappy.sse.SseBroadcaster;
import io.joshworks.snappy.sse.SseCallback;
import io.undertow.server.handlers.sse.ServerSentEventConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SubscriptionEndpoint {

    public static final String PATH_PARAM_STREAM = "stream";

    private final EventStore store;
    private final EventBroadcaster broadcast;

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionEndpoint.class);

    private Map<ServerSentEventConnection, PollingSubscriber<EventRecord>> pollers = new HashMap<>();

    public SubscriptionEndpoint(EventStore store, EventBroadcaster broadcast) {
        this.store = store;
        this.broadcast = broadcast;
    }

    public SseCallback newPushHandler() {
        return new SseCallback() {
            @Override
            public void connected(ServerSentEventConnection connection, String lastEventId) {
                Map<String, Deque<String>> parameters = connection.getQueryParameters();
                Set<String> streams = parameters.getOrDefault(PATH_PARAM_STREAM, new ArrayDeque<>())
                        .stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());

                if(streams.isEmpty()) {
                    try {
                        connection.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                logger.info("Adding push client for streams: {}", Arrays.toString(streams.toArray()));

                //TODO implement checkpoint
//              int version = lastEventId == null || lastEventId.isEmpty() ? 0 : Integer.valueOf(lastEventId);
                PollingSubscriber<EventRecord> poller = store.poller(streams);
                pollers.put(connection, poller);
                broadcast.add(poller);

                for (String stream : streams) {
                    SseBroadcaster.addToGroup(stream, connection);
                }

            }

            @Override
            public void onClose(ServerSentEventConnection connection) {
                PollingSubscriber<EventRecord> poller = pollers.get(connection);
                pollers.remove(connection);
                broadcast.remove(poller);
            }
        };

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
