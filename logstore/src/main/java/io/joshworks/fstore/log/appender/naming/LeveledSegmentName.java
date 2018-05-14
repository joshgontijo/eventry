package io.joshworks.fstore.log.appender.naming;

public class LeveledSegmentName extends ShortUUIDNamingStrategy {

    private final NamingStrategy strategy;
    private final int level;
    private final int levelPos;

    public LeveledSegmentName(NamingStrategy strategy, int level, int levelPos) {
        this.strategy = strategy;
        this.level = level;
        this.levelPos = levelPos;
    }

    @Override
    public String prefix() {
        String prefix = strategy.prefix();

        String format = "%06d";
        return prefix + "-" + String.format(format, level) + "_" + levelPos;
    }
}
