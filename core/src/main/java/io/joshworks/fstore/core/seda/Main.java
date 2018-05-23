package io.joshworks.fstore.core.seda;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {

        SedaContext context = new LocalSedaContext();

        context.addStage(String.class, new Stage.Builder<>("first-stage", ctx -> {
            System.out.println("Data: " + ctx.data);
            ctx.submit(ctx.data.length());
        }));


        context.addStage(Integer.class, new Stage.Builder<>("second-stage", event -> {
            System.out.println("Size: " + event.data);
        }));


        context.submit("Yolo");

        context.close();

    }
}
