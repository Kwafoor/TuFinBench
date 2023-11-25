package cn.junbo.task1;

import cn.junbo.model.DoubleResult;
import cn.junbo.model.VertexType;
import cn.junbo.task1.algorithms.LoanSumAlgorithms;
import cn.junbo.utils.CsvFileSource;
import cn.junbo.utils.SinkFunctionFactory;
import cn.junbo.utils.SortFileSink;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.function.FileSink;
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

public class LoanSum {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoanSum.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/";
    public static final String SOURCE_FILE_PATH = "finBench/";
    public static final String SOURCE_DIR = "source.dir";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        if (args.length > 1) {
            envConfig.put(FileSink.OUTPUT_DIR, args[0]);
            envConfig.put(SOURCE_DIR, args[1]);
            envConfig.put(SortFileSink.TASK_ID, "1");
        } else {
            envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
            envConfig.put(SOURCE_DIR, SOURCE_FILE_PATH);
            envConfig.put(SortFileSink.TASK_ID, "1");
        }

        IPipelineResult result = LoanSum.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        String dataPath = environment.getEnvironmentContext().getConfig().getString(SOURCE_DIR);
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            PWindowSource<IVertex<String, Tuple<VertexType, Double>>> personVertices =
                    pipelineTaskCxt.buildSource(new CsvFileSource<>(dataPath + "Person.csv",
                                    line -> {
                                        String[] fields = line.split("\\|");
                                        IVertex<String, Tuple<VertexType, Double>> vertex = new ValueVertex<>(
                                                String.valueOf(fields[0]), Tuple.of(VertexType.PERSON, 0.0));
                                        return Collections.singletonList(vertex);
                                    }), AllWindow.getInstance())
                            .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IVertex<String, Tuple<VertexType, Double>>> accountVertices =
                    pipelineTaskCxt.buildSource(new CsvFileSource<>(dataPath + "Account.csv",
                                    line -> {
                                        String[] fields = line.split("\\|");
                                        IVertex<String, Tuple<VertexType, Double>> vertex = new ValueVertex<>(
                                                String.valueOf(fields[0]), Tuple.of(VertexType.ACCOUNT, 0.0));
                                        return Collections.singletonList(vertex);
                                    }), AllWindow.getInstance())
                            .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IVertex<String, Tuple<VertexType, Double>>> loanVertices =
                    pipelineTaskCxt.buildSource(new CsvFileSource<>(dataPath + "Loan.csv",
                                    line -> {
                                        String[] fields = line.split("\\|");
                                        IVertex<String, Tuple<VertexType, Double>> vertex = new ValueVertex<>(
                                                String.valueOf(fields[0]), Tuple.of(VertexType.LOAN, Double.valueOf(fields[1])));
                                        return Collections.singletonList(vertex);
                                    }), AllWindow.getInstance())
                            .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));


            PWindowSource<IEdge<String, Integer>> loanEdges = pipelineTaskCxt.buildSource(new CsvFileSource<>(dataPath + "LoanDepositAccount.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<String, Integer> edge = new ValueEdge<>(String.valueOf(fields[0]), String.valueOf(fields[1]), 1);
                                return Collections.singletonList(edge);
                            }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<String, Integer>> transferEdges = pipelineTaskCxt.buildSource(new CsvFileSource<>(dataPath + "AccountTransferAccount.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<String, Integer> edge = new ValueEdge<>(String.valueOf(fields[0]), String.valueOf(fields[1]), 1);
                                return Collections.singletonList(edge);
                            }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<String, Integer>> ownEdges = pipelineTaskCxt.buildSource(new CsvFileSource<>(dataPath + "PersonOwnAccount.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<String, Integer> edge = new ValueEdge<>(String.valueOf(fields[1]), String.valueOf(fields[0]), 1);
                                return Collections.singletonList(edge);
                            }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));


            int iterationParallelism = conf.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(2)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();
            PGraphWindow<String, Tuple<VertexType, Double>, Integer> graphWindow =
                    pipelineTaskCxt.buildWindowStreamGraph(personVertices.union(accountVertices).union(loanVertices),
                            loanEdges.union(transferEdges).union(ownEdges), graphViewDesc);

            SinkFunction<DoubleResult> sink = SinkFunctionFactory.getSinkFunction(conf);
            graphWindow.compute(new LoanSumAlgorithms(4))
                    .compute(iterationParallelism)
                    .getVertices()
                    .filter(v -> v.getValue().f0 == VertexType.PERSON && v.getValue().f1 > 0)
                    .map(v -> new DoubleResult(new BigInteger(v.getId()), v.getValue().f1 / 100000000))
                    .sink(sink)
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
        });
        return pipeline.execute();
    }
}
