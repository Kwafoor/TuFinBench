package cn.junbo.task1.algorithms;

import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;

import java.util.Iterator;
import java.util.List;

public class MSumVertexCentricComputeFunction extends AbstractVcFunc<String, Double, Double, Double> {
    @Override
    public void compute(String vertexId,
                        Iterator<Double> messageIterator) {
        IVertex<String, Double> vertex = this.context.vertex().get();
        List<IEdge<String, Double>> outEdges = context.edges().getOutEdges();
        if (this.context.getIterationId() == 1) {
            if (!outEdges.isEmpty()) {
                for (IEdge<String, Double> edge : this.context.edges().getOutEdges()) {
                    this.context.sendMessage(edge.getTargetId(), edge.getValue());
                }
            }

        } else {
            double loadSum = 0;
            while (messageIterator.hasNext()) {
                double value = messageIterator.next();
                loadSum += value;
            }
            double pr = loadSum ;
            this.context.setNewVertexValue(pr);

            for (IEdge<String, Double> edge : this.context.edges().getOutEdges()) {
                this.context.sendMessage(edge.getTargetId(), edge.getValue());
            }
        }
    }
}
