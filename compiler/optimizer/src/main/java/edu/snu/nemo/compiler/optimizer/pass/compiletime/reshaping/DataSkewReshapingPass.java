/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DecoderProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.EncoderProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.AggregationBarrierVertex;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Pass that adds an {@link AggregationBarrierVertex} before Shuffle edges.
 */
public final class DataSkewReshapingPass extends ReshapingPass {
  /**
   * Default constructor.
   */
  public DataSkewReshapingPass() {
    super(Collections.singleton(DataCommunicationPatternProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final List<AggregationBarrierVertex> metricCollectionVertices = new ArrayList<>();

    dag.topologicalDo(v -> {
      // We care about OperatorVertices that have any incoming edges that are of type Shuffle.
      if (v instanceof OperatorVertex && dag.getIncomingEdgesOf(v).stream().anyMatch(irEdge ->
          DataCommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getPropertyValue(DataCommunicationPatternProperty.class).get()))) {
        final AggregationBarrierVertex<Integer, Long> aggregationBarrierVertex
            = new AggregationBarrierVertex<>();
        metricCollectionVertices.add(aggregationBarrierVertex);
        builder.addVertex(v);
        builder.addVertex(aggregationBarrierVertex);
        dag.getIncomingEdgesOf(v).forEach(edge -> {
          // we insert the metric collection vertex when we meet a shuffle edge
          if (DataCommunicationPatternProperty.Value.Shuffle
                .equals(edge.getPropertyValue(DataCommunicationPatternProperty.class).get())) {
            // We then insert the AggregationBarrierVertex between the vertex and incoming vertices.
            final IREdge newEdge = new IREdge(DataCommunicationPatternProperty.Value.OneToOne,
                edge.getSrc(), aggregationBarrierVertex);
            newEdge.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
            newEdge.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));

            final IREdge edgeToReducer = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
                aggregationBarrierVertex, v, edge.isSideInput());
            edge.copyExecutionPropertiesTo(edgeToReducer);
            builder.connectVertices(newEdge);
            builder.connectVertices(edgeToReducer);
          } else {
            builder.connectVertices(edge);
          }
        });
      } else { // Others are simply added to the builder, unless it comes from an updated vertex
        builder.addVertex(v);
        dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      }
    });
    final DAG<IRVertex, IREdge> newDAG = builder.build();
    metricCollectionVertices.forEach(v -> v.setDAGSnapshot(newDAG));
    return newDAG;
  }
}
