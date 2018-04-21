package io.joshworks.fstore.log.appender.naming;

import java.util.List;

public interface NamingStrategy {

    String name(List<String> currentSegments);

}
