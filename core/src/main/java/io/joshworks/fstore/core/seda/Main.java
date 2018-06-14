package io.joshworks.fstore.core.seda;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {


        SedaContext context = new SedaContext();
        try {

            context.addStage("stage-22", (StageHandler<String>) elem -> elem.complete("!!!"), new Stage.Builder());
            context.addStage("stage-0", (StageHandler<String>) elem -> elem.submit("stage-2", elem.data.length()), new Stage.Builder());
            context.addStage("stage-1", new TestHandler(), new Stage.Builder());
            context.addStage("stage-2", new TestHandler2(),  new Stage.Builder().queueSize(20).maximumPoolSize(1).blockWhenFull());


            CompletableFuture<Object> complete = context.submit("stage-0", "aaa");
            complete.exceptionally(t -> {
                System.err.println(t);
                return null;
            }).thenAccept(System.out::println);
//            for (int i = 0; i < 20; i++) {
//                context.submit("stage-0", String.valueOf(i));
//            }


        } finally {
            context.shutdown();


//            System.out.println(sum.get());
            System.err.println(context.stats());
        }

    }

    private static class TestHandler implements StageHandler<String> {

        @Override
        public void onEvent(EventContext<String> context) {
            context.submit("stage-2", Integer.parseInt(context.data));
        }
    }

    private static class TestHandler2 implements StageHandler<Integer> {

        @Override
        public void onEvent(EventContext<Integer> context) {
            System.out.println(context.data);
            context.complete(context.data);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
