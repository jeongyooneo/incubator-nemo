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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.composite;

import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping.DataSkewReshapingPass;

import java.util.Arrays;

/**
 * Pass to annotate and reshape the IRDAG for data skew handling.
 * It adds an AggregationBarrierVertex after Shuffle edges that aggregates metric and acts as a barrier.
 * The metrics are used in runtime skew handling, i.e. key range-based shuffle re-partitioning.
 *
 * NOTE: we currently put the DataSkewCompositePass at the end of the list for each policies,
 * as it needs to take a snapshot at the end of the pass.
 * This could be prevented by modifying other passes to take the snapshot of the DAG
 * at the end of each passes for AggregationCollectionVertices.
 */
public final class DataSkewCompositePass extends CompositePass {
  /**
   * Default constructor.
   */
  public DataSkewCompositePass() {
    super(Arrays.asList(
        new DataSkewReshapingPass(),
        new DataSkewVertexDynamicOptimizationPass(),
        new DataSkewEdgeDataStorePass(),
        new DataSkewEdgeDynamicOptimizationPass(),
        new DataSkewEdgePartitionerPass()
    ));
  }
}
