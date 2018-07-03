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
package edu.snu.nemo.common.ir.vertex;

/**
 * IRVertex that collects statistics to send them to the optimizer for dynamic optimization.
 * This class is generated in the DAG through
 * {edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.DataSkewCompositePass}.
 */
public final class PartitionSizeAggregationBarrierVertex<K extends String, V extends Long>
    extends AggregationBarrierVertex<K, V> {

  /**
   * Constructor for dynamic optimization vertex.
   */
  public PartitionSizeAggregationBarrierVertex() {
    super();
  }
  
  @Override
  public void aggregateMetricData(final K key, final V value) {
    if (metricData.containsKey(key)) {
      Long existingValue = metricData.get(key);
      Long newValue = existingValue + (Long) value;
      metricData.put(key, (V) newValue);
    } else {
      metricData.put(key, value);
    }
  }
}
