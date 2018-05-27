package io.joshworks.fstore.core.seda;

import java.util.concurrent.atomic.AtomicLong;

public class Main {

    public static void main(String[] args) {

        AtomicLong sum = new AtomicLong();
        SedaContext context = new SedaContext();
        try {

            context.addStage(String.class, new Stage.Builder<>("first-stage", ctx -> ctx.publish(ctx.data.length())));
            context.addStage(Integer.class, new Stage.Builder<>("second-stage", event -> {
                sum.addAndGet(event.data);
                Thread.sleep(1000);
                event.publish(1L);
            }));
            context.addStage(Long.class, new Stage.Builder<>("third-stage", event -> {
                Thread.sleep(1000);
                System.out.println(event.data);
            }));


            for (int i = 0; i < 10; i++) {
                context.submit("a");
            }


        } finally {
            context.shutdown();

            System.out.println(sum.get());
            System.err.println(context.stats());
        }


    }
}
