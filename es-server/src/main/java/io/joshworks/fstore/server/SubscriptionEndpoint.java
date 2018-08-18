package io.joshworks.fstore.server;

import io.joshworks.fstore.es.EventStore;
import io.joshworks.snappy.http.HttpExchange;
import io.joshworks.snappy.sse.SseBroadcaster;
import io.undertow.server.handlers.sse.ServerSentEventConnectionCallback;

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SubscriptionEndpoint {

    public static final String PATH_PARAM_SUBSCRIPTION_ID = "subscriptionId";
    public static final String QUERY_PARAM_STREAM = "stream";
    public static final String ALL_GROUP = "stream";

    private final EventStore store;
    private final EventBroadcast broadcast;
    //subscriptionId - [streamName]
    private final Map<String, Set<String>> subscription = new HashMap<>();

    public SubscriptionEndpoint(EventStore store) {
        this.store = store;
        this.broadcast = new EventBroadcast(store);
    }

    public void create(HttpExchange exchange) {

    }

    public ServerSentEventConnectionCallback newPushHandler() {
        return (connection, lastEventId) -> {
            String subscriptionId = connection.getParameter(PATH_PARAM_SUBSCRIPTION_ID);
            Map<String, Deque<String>> queryParams = connection.getQueryParameters();
            Deque<String> streamParams = queryParams.get(QUERY_PARAM_STREAM);
            HashSet<String> streams = new HashSet<>(streamParams);
            subscription.put(subscriptionId, streams);

            for (String stream : streams) {
                SseBroadcaster.addToGroup(stream, connection);
            }
            if(streams.isEmpty()) {
                SseBroadcaster.addToGroup(ALL_GROUP, connection);
            }
        };
    }


}
