package cn.junbo.utils;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory;

public class SinkFunctionFactory extends ExampleSinkFunctionFactory {

    public static <T> SinkFunction<T> getSinkFunction(Configuration configuration) {
        return (SinkFunction) new SortFileSink<>();
    }
}
