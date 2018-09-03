package io.joshworks.fstore.server;

import io.joshworks.fstore.es.EventStore;

import java.io.File;

import static io.joshworks.snappy.SnappyServer.cors;
import static io.joshworks.snappy.SnappyServer.delete;
import static io.joshworks.snappy.SnappyServer.get;
import static io.joshworks.snappy.SnappyServer.group;
import static io.joshworks.snappy.SnappyServer.onShutdown;
import static io.joshworks.snappy.SnappyServer.post;
import static io.joshworks.snappy.SnappyServer.sse;
import static io.joshworks.snappy.SnappyServer.start;
import static io.joshworks.snappy.parser.MediaTypes.consumes;

public class Main {


    public static void main(String[] args) {

        EventStore store = EventStore.open(new File("J:\\event-store-app"));

        EventBroadcaster broadcast = new EventBroadcaster(2000, 3);
        SubscriptionEndpoint subscriptions = new SubscriptionEndpoint(store, broadcast);
        StreamEndpoint streams = new StreamEndpoint(store);
        ProjectionsEndpoint projections = new ProjectionsEndpoint(store);


        group("/streams", () -> {
            post("/", streams::create);
            get("/", streams::streamsQuery);
            get("/metadata", streams::listStreams);

            group("{streamId}", () -> {
                get(streams::fetchStreams);
                post(streams::append);
                delete(streams::delete);
                get("/metadata", streams::metadata);
            });
        });

        group("/projections", () -> {
            get(projections::getAll);
            group("{name}", () -> {
                post(projections::create, consumes("application/javascript"));
                post("/run", projections::run);
                get(projections::get);
            });
        });

        group("/push", () -> sse(subscriptions.newPushHandler()));


        onShutdown(store::close);

        cors();
        start();

    }
}
