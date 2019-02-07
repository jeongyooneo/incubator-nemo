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
package org.apache.nemo.runtime.master.scheduler.constraint;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.executionproperty.AssociatedProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.StaticDisaggProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

/**
 * This constraint is to follow {@link StaticDisaggProperty}.
 */
@AssociatedProperty(StaticDisaggProperty.class)
public final class StaticDisaggSchedulingConstraint implements SchedulingConstraint {
  private static final Logger LOG = LoggerFactory.getLogger(StaticDisaggSchedulingConstraint.class.getName());
  
  @Inject
  private StaticDisaggSchedulingConstraint() {
  }
  
  private void getNodeName(final Map<String, Integer> propertyValue, final int taskIndex) {
    /*
    final List<String> nodeNames = new ArrayList<>(propertyValue.keySet());
    Collections.sort(nodeNames, Comparator.naturalOrder());
    int index = taskIndex;
    if (nodeNames.size() == 1) {
      LOG.info("NodeName {} index {}", nodeNames.get(0), index);
      return nodeNames.get(0);
    } else {
      for (final String nodeName : nodeNames) {
        LOG.info("NodeName {} index {} propertyValue {}",
          nodeName, index, propertyValue.get(nodeName).right());
        if (index >= propertyValue.get(nodeName).right()) {
          index -= propertyValue.get(nodeName).right();
        } else {
          return nodeName;
        }
      }
    }
    throw new
      IllegalStateException("Detected excessive parallelism which ResourceSiteProperty does not cover");
      */
  }
  
  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    /*
    final Map<String, Integer> propertyValue = task.getPropertyValue(StaticDisaggProperty.class)
      .orElseThrow(() -> new RuntimeException("StaticDisaggProperty expected"));
    if (propertyValue.isEmpty()) {
      return true;
    }
    try {
      return executor.getNodeName().equals(
        getNodeName(propertyValue, RuntimeIdManager.getIndexFromTaskId(task.getTaskId())));
    } catch (final IllegalStateException e) {
      throw new RuntimeException(String.format("Cannot schedule %s", task.getTaskId(), e));
    }
    */
    return true;
  }
}
