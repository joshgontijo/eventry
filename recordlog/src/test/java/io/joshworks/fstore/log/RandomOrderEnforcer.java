package io.joshworks.fstore.log;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RandomOrderEnforcer extends BlockJUnit4ClassRunner {

    public RandomOrderEnforcer(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected List<FrameworkMethod> computeTestMethods() {
        List<FrameworkMethod> methods = new ArrayList<>(super.computeTestMethods());
        Collections.shuffle(methods);
        return methods;
    }
}