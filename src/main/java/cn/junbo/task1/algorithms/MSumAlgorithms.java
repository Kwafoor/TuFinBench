package cn.junbo.task1.algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;

public class MSumAlgorithms extends VertexCentricCompute<String, Double, Double, Double> {

    public MSumAlgorithms(long iterations) {
        super(iterations);
    }

    @Override
    public VertexCentricComputeFunction<String, Double, Double, Double> getComputeFunction() {
        return new MSumVertexCentricComputeFunction();
    }


    @Override
    public VertexCentricCombineFunction<Double> getCombineFunction() {
        return null;
    }
}
