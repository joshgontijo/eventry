package io.joshworks.fstore.log.appender.naming;

import java.util.List;

public interface NamingStrategy {

    String DEFAULT_EXNTENSION = ".lsm";

    String name(List<String> currentSegments);

}
