package io.joshworks.fstore.server;

import io.joshworks.fstore.es.EventStore;

import java.io.File;

import static io.joshworks.snappy.SnappyServer.delete;
import static io.joshworks.snappy.SnappyServer.get;
import static io.joshworks.snappy.SnappyServer.group;
import static io.joshworks.snappy.SnappyServer.onShutdown;
import static io.joshworks.snappy.SnappyServer.post;
import static io.joshworks.snappy.SnappyServer.sse;
import static io.joshworks.snappy.SnappyServer.start;

public class Main {


    public static void main(String[] args) {

        EventStore store  = EventStore.open(new File("J:\\event-store-app"));

        StreamEndpoint streams = new StreamEndpoint(store);
        SubscriptionEndpoint subscriptions = new SubscriptionEndpoint(store);

        group("/streams", () -> {
            post("/", streams::create);
            get("/", streams::listStreams);

            group("{streamId}", () -> {
                get(streams::fetchStreams);
                post(streams::append);
                delete(streams::delete);
                get("/metadata", streams::metadata);

                sse("/");

            });
        });

        group("/subscriptions", () -> {
            post(subscriptions::create);
            sse("{subscriptionId}/push", subscriptions.newPushHandler());
        });


        onShutdown(store::close);

        start();

    }
}
