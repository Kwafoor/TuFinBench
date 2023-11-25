package cn.junbo.task2;

import cn.junbo.model.Result;
import cn.junbo.task2.algorithms.LoopTransferAlgorithms;
import cn.junbo.utils.CsvFileSource;
import cn.junbo.utils.SinkFunctionFactory;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Collections;

public class LoopTransfer {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoopTransfer.class);

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = LoopTransfer.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();

            PWindowSource<IVertex<BigInteger, Long>> accountVertices =
                    pipelineTaskCxt.buildSource(new CsvFileSource<>("Account.csv",
                                    line -> {
                                        String[] fields = line.split("\\|");
                                        IVertex<BigInteger, Long> vertex = new ValueVertex<>(
                                                new BigInteger(fields[0]), 0L);
                                        return Collections.singletonList(vertex);
                                    }), AllWindow.getInstance())
                            .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));


            PWindowSource<IEdge<BigInteger, Integer>> transferEdges = pipelineTaskCxt.buildSource(new CsvFileSource<>("AccountTransferAccount.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<BigInteger, Integer> edge = new ValueEdge<>(new BigInteger(fields[0]), new BigInteger(fields[1]), 1);
                                return Collections.singletonList(edge);
                            }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));


            int iterationParallelism = conf.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(2)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();

            PGraphWindow<BigInteger, Long, Integer> graphWindow =
                    pipelineTaskCxt.buildWindowStreamGraph(accountVertices,
                            transferEdges, graphViewDesc);

            SinkFunction<Result> sink = SinkFunctionFactory.getSinkFunction(conf);
            graphWindow.compute(new LoopTransferAlgorithms(4))
                    .compute(iterationParallelism)
                    .getVertices()
                    .filter(v -> v.getValue()>0)
                    .map(v -> new Result(v.getId(), v.getValue()))
                    .sink(sink)
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
        });
        return pipeline.execute();
    }
}
