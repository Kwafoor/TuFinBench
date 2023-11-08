package cn.junbo.task2.algorithms;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.graph.vertex.IVertex;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class LoopTransferVertexCentricComputeFunction extends AbstractVcFunc<BigInteger, Long, Integer, List<Tuple<BigInteger, Integer>>> {
    @Override
    public void compute(BigInteger vertexId,
                        Iterator<List<Tuple<BigInteger, Integer>>> messageIterator) {
        IVertex<BigInteger, Long> vertex = this.context.vertex().get();

        if (this.context.getIterationId() == 1) {
            this.context.sendMessageToNeighbors(Arrays.asList(Tuple.of(vertexId, 1)));
        } else if (this.context.getIterationId() == 2) {
            HashMap<BigInteger, Integer> srcCount = new HashMap<>();
            while (messageIterator.hasNext()) {
                List<Tuple<BigInteger, Integer>> ms = messageIterator.next();
                ms.forEach(t -> {
                    if (srcCount.containsKey(t.f0)) {
                        srcCount.put(t.f0, srcCount.get(t.f0) + t.f1);
                    } else {
                        srcCount.put(t.f0, t.f1);
                    }
                });
            }
            List<Tuple<BigInteger, Integer>> sendMs = new ArrayList<>();
            srcCount.forEach((srcId, cnt) -> sendMs.add(Tuple.of(srcId, cnt)));
            this.context.sendMessageToNeighbors(sendMs);
        } else if (this.context.getIterationId() == 3) {
            AtomicReference<Long> distinctLoop = new AtomicReference<>(0L);
            while (messageIterator.hasNext()) {
                List<Tuple<BigInteger, Integer>> ms = messageIterator.next();
                ms.forEach(t -> {
                    if (t.f0.compareTo(vertexId) == 0) {
                        distinctLoop.getAndSet(distinctLoop.get() + 1);
                    }
                });
            }
            vertex.withValue(distinctLoop.get());
        }
    }
}
