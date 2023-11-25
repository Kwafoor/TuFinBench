package cn.junbo.task3;

import cn.junbo.model.DoubleResult;
import cn.junbo.utils.CsvFileSource;
import cn.junbo.utils.SinkFunctionFactory;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
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

import java.math.BigInteger;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class QuickTransfer {

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = QuickTransfer.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            PWindowSource<IVertex<BigInteger, Double>> accountVertices =
                    pipelineTaskCxt.buildSource(new CsvFileSource<>("Account.csv",
                                    line -> {
                                        String[] fields = line.split("\\|");
                                        IVertex<BigInteger, Double> vertex = new ValueVertex<>(
                                                new BigInteger(fields[0]), 0d);
                                        return Collections.singletonList(vertex);
                                    }), AllWindow.getInstance())
                            .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));


            PWindowSource<IEdge<BigInteger, Double>> transferOutEdges = pipelineTaskCxt.buildSource(new CsvFileSource<>("AccountTransferAccount.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<BigInteger, Double> edge = new ValueEdge<>(new BigInteger(fields[0]), new BigInteger(fields[1]), Double.valueOf(fields[2]));
                                return Collections.singletonList(edge);
                            }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));


            PWindowSource<IEdge<BigInteger, Double>> transferInEdges = pipelineTaskCxt.buildSource(new CsvFileSource<>("AccountTransferAccount.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<BigInteger, Double> edge = new ValueEdge<>(new BigInteger(fields[1]), new BigInteger(fields[0]), Double.valueOf(fields[2]), EdgeDirection.IN);
                                return Collections.singletonList(edge);
                            }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(2)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();

            PGraphWindow<BigInteger, Double, Double> graphWindow =
                    pipelineTaskCxt.buildWindowStreamGraph(accountVertices,
                            transferOutEdges.union(transferInEdges), graphViewDesc);

            SinkFunction<DoubleResult> sink = SinkFunctionFactory.getSinkFunction(conf);
            graphWindow.compute(new QuickTransferAlgorithms(1))
                    .compute(iterationParallelism)
                    .getVertices()
                    .filter(v -> v.getValue() > 0)
                    .map(v -> new DoubleResult(v.getId(), v.getValue()))
                    .sink(sink)
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
        });
        return pipeline.execute();
    }

    public static class QuickTransferAlgorithms extends VertexCentricCompute<BigInteger, Double, Double, Double> {

        public QuickTransferAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<BigInteger, Double, Double, Double> getComputeFunction() {
            return new QuickTransferComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Double> getCombineFunction() {
            return null;
        }
    }

    public static class QuickTransferComputeFunction extends AbstractVcFunc<BigInteger, Double, Double, Double> {

        @Override
        public void compute(BigInteger vertexId,
                            Iterator<Double> messageIterator) {
            List<IEdge<BigInteger, Double>> outEdges = context.edges().getOutEdges();
            List<IEdge<BigInteger, Double>> inEdges = context.edges().getInEdges();

            AtomicReference<Double> inSum = new AtomicReference<>(0d);
            AtomicReference<Double> outSum = new AtomicReference<>(0d);
            outEdges.forEach(o -> outSum.updateAndGet(v -> v + o.getValue()));
            inEdges.forEach(i -> inSum.updateAndGet(v -> v + i.getValue()));
            if (outEdges.isEmpty() || inEdges.isEmpty()) {
                context.setNewVertexValue(0d);
            } else {
                context.setNewVertexValue(inSum.get() / outSum.get());
            }
        }

    }

}
