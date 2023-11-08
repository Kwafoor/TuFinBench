package cn.junbo.task2.algorithms;

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.common.tuple.Tuple;

import java.math.BigInteger;
import java.util.List;

public class LoopTransferAlgorithms extends VertexCentricCompute<BigInteger, Long, Integer,List<Tuple<BigInteger,Integer>>> {

    public LoopTransferAlgorithms(long iterations) {
        super(iterations);
    }

    @Override
    public VertexCentricComputeFunction<BigInteger, Long, Integer,List<Tuple<BigInteger,Integer>>> getComputeFunction() {
        return new LoopTransferVertexCentricComputeFunction();
    }


    @Override
    public VertexCentricCombineFunction<List<Tuple<BigInteger,Integer>>> getCombineFunction() {
        return null;
    }
}
