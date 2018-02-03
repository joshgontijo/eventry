package io.joshworks.fstore;

import io.joshworks.fstore.event.Event;
import io.joshworks.fstore.serializer.KryoEventSerializer;
import io.joshworks.fstore.store.RandomAccessDataStore;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.File;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
public class BenchmarkRunner {

    private EventStore store;
    private static final String FILE_NAME = "BENCHMARK.DAT";

    @Test
    public void benchmark() throws RunnerException {
        Options opt = new OptionsBuilder().include("io.joshworks.fstore")
                .mode(Mode.Throughput)
                .timeUnit(TimeUnit.SECONDS)
                .warmupTime(TimeValue.seconds(1))
                .warmupIterations(1)
                .measurementTime(TimeValue.microseconds(1))
                .measurementIterations(10)
                .threads(1)
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                // .jvmArgs("-XX:+UnlockDiagnosticVMOptions",
                // "-XX:+PrintInlining")
                // .addProfiler(WinPerfAsmProfiler.class)
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setup(){
        store = new EventStore(new RandomAccessDataStore(FILE_NAME), new KryoEventSerializer());
    }

    @TearDown
    public void cleanup() {
        File file = new File(FILE_NAME);
        if(file.exists()) {
            file.delete();
        }
    }

    @Benchmark
    public void write(Blackhole bh)  {
        String uuid = UUID.randomUUID().toString();
        String someRand = uuid.substring(0, 8);
        Event event = Event.create(uuid, Map.of("k1", someRand), someRand);
        store.save(event);
    }

    @Benchmark
    public void read(Blackhole bh)  {
        //TODO
    }



}