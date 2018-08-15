package io.joshworks.fstore.server;

import static io.joshworks.snappy.SnappyServer.delete;
import static io.joshworks.snappy.SnappyServer.get;
import static io.joshworks.snappy.SnappyServer.group;
import static io.joshworks.snappy.SnappyServer.post;
import static io.joshworks.snappy.SnappyServer.start;

public class Main {


    public static void main(String[] args) {

        StreamEndpoint streams = new StreamEndpoint(null);

        group("/streams", () -> {
            post("/", streams::create);
            get("/", streams::list);

            group("{streamId}", () -> {
                get(streams::fromStream);
                post(streams::append);
                delete(streams::delete);
                get("/metadata", streams::metadata);
            });

        });

        start();

    }
}
