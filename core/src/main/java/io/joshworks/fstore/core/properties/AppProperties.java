package io.joshworks.fstore.core.properties;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 * Loads properties from various resources, including classpath.
 * Example: classpath:my-config.properties
 *
 * Loads properties in sequence:
 * 1. Specified by order of the locations provided
 * 2. From System environment using {@link System#getenv()}
 * 3. From System properties using {@link System#getProperties()}
 *
 */
public class AppProperties {

    private static final String CLASSPATH = "classpath:";
    private final Properties properties = new Properties();

    private AppProperties(String... locations) {

        for (String location : locations) {
            try {
                if (location.startsWith(CLASSPATH)) {
                    InputStream inputStream = new URL(null, location, new ClasspathHandler(AppProperties.class.getClassLoader())).openStream();
                    properties.load(inputStream);
                } else {
                    InputStream inputStream = new URL(location).openStream();
                    properties.load(inputStream);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load " + location, e);
            }
        }

        for (Map.Entry<String, String> env : System.getenv().entrySet()) {
            String key = env.getKey().replace('_', '.').toLowerCase();
            properties.setProperty(key, env.getValue());
        }

        Properties systemProps = System.getProperties();
        Set<String> keys = systemProps.stringPropertyNames();
        for (String key : keys) {
            properties.setProperty(key, systemProps.getProperty(key));
        }
    }

    private AppProperties(Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
    }

    public static AppProperties create(String... sources) {
        return new AppProperties(sources);
    }

    /**
     * Creates a new instance by selecting all the keys that starts with the value provided
     */
    public AppProperties allOf(String keyPrefix) {
        Map<String, String> slice = properties.stringPropertyNames().stream()
                .filter(key -> key.startsWith(keyPrefix))
                .collect(Collectors.toMap(Function.identity(), properties::getProperty));
        return new AppProperties(slice);
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(properties.getProperty(key));
    }

    public Optional<Integer> getInt(String key) {
        String val = properties.getProperty(key);
        if (val == null || val.trim().isEmpty()) {
            return Optional.empty();
        }
        int nVal = Integer.parseInt(val);

        return Optional.of(nVal);
    }

    public Optional<Long> getLong(String key) {
        String val = properties.getProperty(key);
        if (val == null || val.trim().isEmpty()) {
            return Optional.empty();
        }
        long nVal = Long.parseLong(val);

        return Optional.of(nVal);
    }

    public Optional<Double> getDouble(String key) {
        String val = properties.getProperty(key);
        if (val == null || val.trim().isEmpty()) {
            return Optional.empty();
        }
        double nVal = Double.parseDouble(val);

        return Optional.of(nVal);
    }

    public Optional<Float> getFloat(String key) {
        String val = properties.getProperty(key);
        if (val == null || val.trim().isEmpty()) {
            return Optional.empty();
        }
        float nVal = Float.parseFloat(val);

        return Optional.of(nVal);
    }

    public Optional<Boolean> getBoolean(String key) {
        String val = properties.getProperty(key);
        if (val == null || val.trim().isEmpty()) {
            return Optional.empty();
        }
        Boolean bool = Boolean.parseBoolean(val);

        return Optional.of(bool);
    }


    public class ClasspathHandler extends URLStreamHandler {
        private final ClassLoader classLoader;

        public ClasspathHandler() {
            this.classLoader = getClass().getClassLoader();
        }

        public ClasspathHandler(ClassLoader classLoader) {
            this.classLoader = classLoader;
        }

        @Override
        protected URLConnection openConnection(URL url) throws IOException {
            final URL resourceUrl = classLoader.getResource(url.getPath());
            if (resourceUrl == null) {
                throw new IOException("Resource not found: " + url);
            }
            return resourceUrl.openConnection();
        }
    }

}
