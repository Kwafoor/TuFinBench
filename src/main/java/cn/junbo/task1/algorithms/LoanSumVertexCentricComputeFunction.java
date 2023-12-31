package cn.junbo.task1.algorithms;

import cn.junbo.model.EdgeType;
import cn.junbo.model.VertexType;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.graph.vertex.IVertex;

import java.util.Iterator;

public class LoanSumVertexCentricComputeFunction extends AbstractVcFunc<String, Tuple<VertexType, Double>, Integer, Tuple<EdgeType, Double>> {
    @Override
    public void compute(String vertexId,
                        Iterator<Tuple<EdgeType, Double>> messageIterator) {
        IVertex<String, Tuple<VertexType, Double>> vertex = this.context.vertex().get();

        if (this.context.getIterationId() == 1 && vertex.getValue().f0 == VertexType.LOAN) {
            this.context.sendMessageToNeighbors(Tuple.of(EdgeType.LOAN, vertex.getValue().f1));
        } else if (this.context.getIterationId() == 2 && vertex.getValue().f0 == VertexType.ACCOUNT) {
            double loadSum = 0;
            while (messageIterator.hasNext() ) {
                Tuple<EdgeType, Double> ms = messageIterator.next();
                if (ms.f0 == EdgeType.LOAN){
                    double value = ms.f1;
                    loadSum += value;
                }
            }
            vertex.getValue().f1 = loadSum;
            this.context.setNewVertexValue(vertex.getValue());
            this.context.sendMessageToNeighbors(Tuple.of(EdgeType.TRANSFER, vertex.getValue().f1));
        } else if (this.context.getIterationId() == 3 && vertex.getValue().f0 == VertexType.ACCOUNT) {
            int transfer = 0;
            double loadSum = 0;
            while (messageIterator.hasNext()) {
                Tuple<EdgeType, Double> ms = messageIterator.next();
                if (ms.f0 == EdgeType.TRANSFER){
                    double value = ms.f1;
                    loadSum += value;
                    transfer++;
                }
            }
            vertex.getValue().f1 = loadSum;
            if (transfer > 0) {
                this.context.setNewVertexValue(vertex.getValue());
                this.context.sendMessageToNeighbors(Tuple.of(EdgeType.OWN, vertex.getValue().f1));
            }
        }else if (this.context.getIterationId() == 4 && vertex.getValue().f0 == VertexType.PERSON) {
            double loadSum = 0;
            while (messageIterator.hasNext()) {
                Tuple<EdgeType, Double> ms = messageIterator.next();
                if (ms.f0 == EdgeType.OWN){
                    double value = ms.f1;
                    loadSum += value;
                }
            }
            vertex.getValue().f1 = loadSum;
        }
    }
}
