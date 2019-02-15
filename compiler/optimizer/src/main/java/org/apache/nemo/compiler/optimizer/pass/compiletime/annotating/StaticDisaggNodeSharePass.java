/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Pass to annotate the IR DAG for skew handling.
 *
 * It marks children and descendents of vertex with {@link ResourceSkewedDataProperty},
 * which collects task-level statistics used for dynamic optimization,
 * with {@link ResourceSkewedDataProperty} to perform skewness-aware scheduling.
 */
@Annotates(StaticDisaggProperty.class)
public final class StaticDisaggNodeSharePass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(StaticDisaggNodeSharePass.class.getName());
  private static final HashMap<String, Integer> EMPTY_MAP = new HashMap<>();
  private static double dramToFlashRatio;
  /**
   * Default constructor.
   */
  public StaticDisaggNodeSharePass() { super(StaticDisaggNodeSharePass.class); }
  
  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dramToFlashRatio = 0.0;
    assignNodeShares(dag, dramToFlashRatio);
    return dag;
  }
  
  private static HashMap<String, Integer> assignShares
    (final int nodeNum, final int parallelism, final double dramRatio) {
    final HashMap<String, Integer> shares = new HashMap<>();
    final int dramTasks = (int) Math.floor(parallelism * dramRatio);
    final int flashTasks = parallelism - dramTasks;
    //final int dramNodes = (int) Math.floor(nodeNum * dramRatio) == 0 ? 1 : (int) Math.floor(nodeNum * dramRatio);
    //final int flashNodes = nodeNum - dramNodes;
  
    //final int dramTasksPerNode = dramTasks / dramNodes;
    //final int dramTasksPerNodeRemainder = dramTasks % dramNodes;
    //final int flashTasksPerNode = flashTasks / flashNodes;
    //final int flashTasksPerNodeRemainder = flashTasks % flashNodes;
    
    LOG.info("DRAM to Flash ratio: {} - parallelism {} dramTasks {} flashTasks {}",
      dramToFlashRatio, parallelism, dramTasks, flashTasks);
  
    shares.put("DRAM", dramTasks);
    shares.put("FLASH", parallelism);
    
    /*
    for (int i = 0; i < nodeNum; i++) {
      if (i < dramNodes) {
        final String node = "121.167.126.206";
        final int assignedTasks = dramTasksPerNode + (i < dramTasksPerNodeRemainder ? 1 : 0);
        shares.put("DRAM", assignedTasks);
        LOG.info("Assigned: {} DRAM {}", node, assignedTasks);
      } else {
        final String node = "121.167.126.206";
        final int assignedTasks = flashTasksPerNode + (i < flashTasksPerNodeRemainder ? 1 : 0);
        shares.put("FLASH", assignedTasks);
        LOG.info("Assigned: {} FLASH {}", node, assignedTasks);
      }
    }
    */

    return shares;
  }
  
  /**
   * @param inEdges list of inEdges to the specific irVertex
   * @return true if and only if the irVertex has one OneToOne edge
   */
  private static boolean isOneToOneEdge(final Collection<IREdge> inEdges) {
    return inEdges.size() == 1 && inEdges.iterator().next()
      .getPropertyValue(CommunicationPatternProperty.class).get()
      .equals(CommunicationPatternProperty.Value.OneToOne);
  }
  
  /**
   * @param dag IR DAG.
   * @param dramRatio DRAM ratio among given nodes.
   */
  private static void assignNodeShares(final DAG<IRVertex, IREdge> dag, final double dramRatio) {
    dag.topologicalDo(irVertex -> {
      final Collection<IREdge> inEdges = dag.getIncomingEdgesOf(irVertex);
      final int parallelism = irVertex.getPropertyValue(ParallelismProperty.class)
        .orElseThrow(() -> new RuntimeException("Parallelism property required"));
      if (inEdges.size() == 0) {
        // Root vertices: data reside in HDFS
        irVertex.setProperty(StaticDisaggProperty.of(EMPTY_MAP));
      } else if (isOneToOneEdge(inEdges)) {
        // Apply parent vertex's setting
        final HashMap<String, Integer> property =
          inEdges.iterator().next().getSrc().getPropertyValue(StaticDisaggProperty.class).get();
        irVertex.setProperty(StaticDisaggProperty.of(property));
      } else {
        // This IRVertex has shuffle inEdge(s), or has multiple inEdges.
        final HashMap<String, Integer> shares = assignShares(2, parallelism, dramRatio);
        irVertex.setProperty(StaticDisaggProperty.of(shares));
      }
    });
  }
}
