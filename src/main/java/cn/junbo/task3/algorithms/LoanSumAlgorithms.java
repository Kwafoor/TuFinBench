package cn.junbo.task3.algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.common.tuple.Tuple;

public class LoanSumAlgorithms extends VertexCentricCompute<String, Tuple<VertexType, Double>, Integer, Tuple<EdgeType, Double>> {

    public LoanSumAlgorithms(long iterations) {
        super(iterations);
    }

    @Override
    public VertexCentricComputeFunction<String, Tuple<VertexType, Double>, Integer, Tuple<EdgeType, Double>> getComputeFunction() {
        return new LoanSumVertexCentricComputeFunction();
    }


    @Override
    public VertexCentricCombineFunction<Tuple<EdgeType, Double>> getCombineFunction() {
        return null;
    }
}
