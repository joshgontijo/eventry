package io.joshworks.fstore.core.seda;

public class Main {

    public static void main(String[] args) {


        SedaContext context = new SedaContext();
        try {

//            context.addStage(String.class, new Stage.Builder<>("first-stage", ctx -> ctx.publish(ctx.data.length())));
            context.addStage("stage-1", new Stage.Builder<>(new TestHandler()));
            context.addStage("stage-2", new Stage.Builder<>(new TestHandler2()).queueSize(20).maximumPoolSize(1).blockWhenFull());
//            context.addStage(Integer.class, new Stage.Builder<>("second-stage", event -> {
//                sum.addAndGet(event.data);
//                Thread.sleep(1000);
//                event.publish(1L);
//            }));
//            context.addStage(Long.class, new Stage.Builder<>("third-stage", event -> {
//                Thread.sleep(1000);
//                System.out.println(event.data);
//            }));


            for (int i = 0; i < 20; i++) {
                context.submit("stage-1", String.valueOf(i));
            }


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
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
