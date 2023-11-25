package cn.junbo.task4;

import cn.junbo.model.DoubleResult;
import cn.junbo.model.EdgeType;
import cn.junbo.model.VertexType;
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
import com.antgroup.geaflow.common.tuple.Triple;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
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
import java.util.Iterator;

public class GuartaanteeSum {

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = GuartaanteeSum.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            PWindowSource<IVertex<BigInteger, Triple<VertexType, Double, Double>>> personVertices =
                    pipelineTaskCxt.buildSource(new CsvFileSource<>("Person.csv",
                                    line -> {
                                        String[] fields = line.split("\\|");
                                        IVertex<BigInteger, Triple<VertexType, Double, Double>> vertex = new ValueVertex<>(
                                                new BigInteger(fields[0]), Triple.of(VertexType.PERSON, 0d, 0d));
                                        return Collections.singletonList(vertex);
                                    }), AllWindow.getInstance())
                            .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IVertex<BigInteger, Triple<VertexType, Double, Double>>> loanVertices =
                    pipelineTaskCxt.buildSource(new CsvFileSource<>("Loan.csv",
                                    line -> {
                                        String[] fields = line.split("\\|");
                                        IVertex<BigInteger, Triple<VertexType, Double, Double>> vertex = new ValueVertex<>(
                                                new BigInteger(fields[0]), Triple.of(VertexType.LOAN, Double.parseDouble(fields[1]), 0d));
                                        return Collections.singletonList(vertex);
                                    }), AllWindow.getInstance())
                            .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));


            PWindowSource<IEdge<BigInteger, EdgeType>> loanToPersonEdges = pipelineTaskCxt.buildSource(new CsvFileSource<>("PersonApplyLoan.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<BigInteger, EdgeType> edge = new ValueEdge<>(new BigInteger(fields[1]), new BigInteger(fields[0]), EdgeType.APPLY_LOAN);
                                return Collections.singletonList(edge);
                            }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));


            PWindowSource<IEdge<BigInteger, EdgeType>> guaranteeInEdges = pipelineTaskCxt.buildSource(new CsvFileSource<>("PersonGuaranteePerson.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<BigInteger, EdgeType> edge = new ValueEdge<>(new BigInteger(fields[1]), new BigInteger(fields[0]), EdgeType.GUARANTEE);
                                return Collections.singletonList(edge);
                            }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(2)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();

            PGraphWindow<BigInteger, Triple<VertexType, Double, Double>, EdgeType> graphWindow =
                    pipelineTaskCxt.buildWindowStreamGraph(loanVertices.union(personVertices),
                            loanToPersonEdges.union(guaranteeInEdges), graphViewDesc);

            SinkFunction<DoubleResult> sink = SinkFunctionFactory.getSinkFunction(conf);
            graphWindow.compute(new GuaranteeAlgorithms(5))
                    .compute(iterationParallelism)
                    .getVertices()
                    .filter(v -> v.getValue().f2 > 0)
                    .map(v -> new DoubleResult(v.getId(), v.getValue().f2))
                    .sink(sink)
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
        });
        return pipeline.execute();
    }

    public static class GuaranteeAlgorithms extends VertexCentricCompute<BigInteger, Triple<VertexType, Double, Double>, EdgeType, Double> {

        public GuaranteeAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<BigInteger, Triple<VertexType, Double, Double>, EdgeType, Double> getComputeFunction() {
            return new QuickTransferComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Double> getCombineFunction() {
            return null;
        }
    }

    public static class QuickTransferComputeFunction extends AbstractVcFunc<BigInteger, Triple<VertexType, Double, Double>, EdgeType, Double> {

        @Override
        public void compute(BigInteger vertexId,
                            Iterator<Double> messageIterator) {
            IVertex<BigInteger, Triple<VertexType, Double, Double>> vertex = this.context.vertex().get();
            if (this.context.getIterationId() == 1 && vertex.getValue().f0 == VertexType.LOAN) {
                this.context.sendMessageToNeighbors(vertex.getValue().f1);
            } else if (this.context.getIterationId() == 2 && vertex.getValue().f0 == VertexType.PERSON) {
                double loanSum = 0d;
                while (messageIterator.hasNext()) {
                    double value = messageIterator.next();
                    loanSum += value;
                }
                vertex.getValue().f1 = loanSum;
                this.context.sendMessageToNeighbors(loanSum);
            } else if (this.context.getIterationId() >= 3 && this.context.getIterationId() <= 4 && vertex.getValue().f0 == VertexType.PERSON) {
                double loanSum = 0d;
                int msCnt = 0;
                while (messageIterator.hasNext()) {
                    double value = messageIterator.next();
                    loanSum += value;
                    msCnt++;
                }
                vertex.getValue().f2 += loanSum;
                if (msCnt > 0) {
                    this.context.sendMessageToNeighbors(vertex.getValue().f1 + vertex.getValue().f2);
                }
            } else if (this.context.getIterationId() == 5 && vertex.getValue().f0 == VertexType.PERSON) {
                double loanSum = 0d;
                while (messageIterator.hasNext()) {
                    double value = messageIterator.next();
                    loanSum += value;
                }
                vertex.getValue().f2 += loanSum;
            }
        }

    }

}
