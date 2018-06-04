package io.joshworks.fstore.core.seda;

public class Main {

    public static void main(String[] args) {


        SedaContext context = new SedaContext();
        try {

            context.addStage("stage-0", (StageHandler<String>) elem -> elem.submit("stage-2", elem.data.length()), new Stage.Builder());
            context.addStage("stage-1", new TestHandler(), new Stage.Builder());
            context.addStage("stage-2", new TestHandler2(),  new Stage.Builder().queueSize(20).maximumPoolSize(1).blockWhenFull());


            for (int i = 0; i < 20; i++) {
                context.submit("stage-0", String.valueOf(i));
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
