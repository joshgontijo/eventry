package io.joshworks.fstore.server;

import io.joshworks.fstore.es.EventStore;
import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.es.stream.StreamInfo;
import io.joshworks.snappy.http.HttpException;
import io.joshworks.snappy.http.HttpExchange;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class StreamEndpoint {

    public static final String QUERY_PARAM_ZIP = "zip";
    public static final String QUERY_PARAM_ZIP_PREFIX = "prefix";
    public static final String PATH_PARAM_STREAM = "streamId";
    private final EventStore store;

    public StreamEndpoint(EventStore store) {
        this.store = store;
    }

    public void create(HttpExchange exchange) {
        NewStream metadataBody = exchange.body().asObject(NewStream.class);
        store.createStream(metadataBody.name, metadataBody.maxCount, metadataBody.maxAge, metadataBody.permissions, metadataBody.metadata);
        exchange.send(201);
    }

    public void fetchStreams(HttpExchange exchange) {
        String stream = exchange.pathParameter(PATH_PARAM_STREAM);


        String zipWithPrefix = extractZipStartingWith(exchange);
        Set<String> streams = extractZipParams(exchange);

        if (!streams.isEmpty() && zipWithPrefix != null) {
            throw new HttpException(400, QUERY_PARAM_ZIP + " and " + QUERY_PARAM_ZIP_PREFIX + " cannot be used together");
        }

        List<EventBody> events;
        if (!streams.isEmpty()) {
            events = store.zipStreams(streams).map(EventBody::from).collect(Collectors.toList());
        } else if (zipWithPrefix != null) {
            events = store.zipStreams(zipWithPrefix).map(EventBody::from).collect(Collectors.toList());
        } else {
            events = store.fromStream(stream).map(EventBody::from).collect(Collectors.toList());
        }

        exchange.send(events);
    }

    //TODO json parsing should be avoided
    public void append(HttpExchange exchange) {
        String stream = exchange.pathParameter(PATH_PARAM_STREAM);
        EventBody eventBody = exchange.body().asObject(EventBody.class);

        //TODO fix toEvent metadata when is empty
        EventRecord event = eventBody.toEvent(stream);
        EventRecord result = store.add(event);

        exchange.send(EventBody.from(result));
    }

    public void delete(HttpExchange exchange) {

    }

    public void listStreams(HttpExchange exchange) {
        List<StreamInfo> streamsMetadata = store.streamsMetadata();
        exchange.send(streamsMetadata);
    }

    public void streamsQuery(HttpExchange exchange) {

        String zipWithPrefix = extractZipStartingWith(exchange);
        Set<String> streams = extractZipParams(exchange);

        if (!streams.isEmpty() && zipWithPrefix != null) {
            throw new HttpException(400, QUERY_PARAM_ZIP + " and " + QUERY_PARAM_ZIP_PREFIX + " cannot be used together");
        }

        //TODO check access to the stream
        List<EventBody> events = new ArrayList<>();
        if (!streams.isEmpty()) {
            events = store.zipStreams(streams).map(EventBody::from).collect(Collectors.toList());
        } else if (zipWithPrefix != null) {
            events = store.zipStreams(zipWithPrefix).map(EventBody::from).collect(Collectors.toList());
        }
        exchange.send(events);
    }

    public void metadata(HttpExchange exchange) {
        String stream = exchange.pathParameter(PATH_PARAM_STREAM);
        Optional<StreamInfo> metadata = store.streamMetadata(stream);
        metadata.ifPresentOrElse(exchange::send, () -> exchange.send(new HttpException(404, "Stream not found for " + stream)));
    }

    private Set<String> extractZipParams(HttpExchange exchange) {
        Deque<String> zip = exchange.queryParameters(QUERY_PARAM_ZIP);
        Set<String> streams = new HashSet<>();
        if (zip != null && !zip.isEmpty()) {
            for (String val : zip) {
                if (val != null && !val.isEmpty()) {
                    streams.add(val);
                }
            }
        }
        return streams;
    }

    private String extractZipStartingWith(HttpExchange exchange) {
        return exchange.queryParameter(QUERY_PARAM_ZIP_PREFIX);
    }
}
