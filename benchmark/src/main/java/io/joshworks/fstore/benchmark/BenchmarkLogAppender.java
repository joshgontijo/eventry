package io.joshworks.fstore.benchmark;

import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.StringSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;


@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
public class BenchmarkLogAppender {


    @State(Scope.Benchmark)
    public static class MyState {

        @Setup(Level.Trial)
        public void doSetup() {
            try {
                File file = Files.createTempDirectory("benchmark").toFile();
                file.deleteOnExit();
                appender = LogAppender.create(new Builder<>(file, new StringSerializer()).segmentSize(Integer.MAX_VALUE));

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println("######## SETUP ###########");
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
            System.out.println("######## TEARDOWN ###########");
        }


        private LogAppender<String> appender;
    }


    @Benchmark
    @Warmup(iterations = 0)
    public void write(MyState state) {
        state.appender.append("a");

    }

}
