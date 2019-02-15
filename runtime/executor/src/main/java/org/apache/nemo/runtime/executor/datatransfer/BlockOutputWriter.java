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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.StaticDisaggProperty;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.block.Block;
import org.apache.nemo.runtime.common.partitioner.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * Represents the output data transfer from a task.
 */
public final class BlockOutputWriter implements OutputWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BlockOutputWriter.class.getName());

  private final RuntimeEdge<?> runtimeEdge;
  private final IRVertex srcIrVertex;
  private final IRVertex dstIrVertex;
  private final Partitioner partitioner;

  private DataStoreProperty.Value blockStoreValue;
  private final BlockManagerWorker blockManagerWorker;
  private final Block blockToWrite;
  private final boolean nonDummyBlock;

  private long writtenBytes;

  /**
   * Constructor.
   *
   * @param srcTaskId           the id of the source task.
   * @param dstIrVertex         the destination IR vertex.
   * @param runtimeEdge         the {@link RuntimeEdge}.
   * @param blockManagerWorker  the {@link BlockManagerWorker}.
   */
  BlockOutputWriter(final String srcTaskId,
                    final int srcTaskIdx,
                    final IRVertex dstIrVertex,
                    final RuntimeEdge<?> runtimeEdge,
                    final BlockManagerWorker blockManagerWorker) {
    this.runtimeEdge = runtimeEdge;
    this.srcIrVertex = ((StageEdge) runtimeEdge).getSrcIRVertex();
    this.dstIrVertex = dstIrVertex;

    this.partitioner = Partitioner.getPartitioner(runtimeEdge);
    this.blockManagerWorker = blockManagerWorker;
  
    // Follow container type(memory or flash-optimized instance)
    // in case of disagg setting for DataStoreProperty value.
    if (srcIrVertex.getPropertyValue(StaticDisaggProperty.class).isPresent()
      && !srcIrVertex.getPropertyValue(StaticDisaggProperty.class).get().isEmpty()) {
      final Map<String, Integer> m = srcIrVertex.getPropertyValue(StaticDisaggProperty.class).get();
      int taskIndex = srcTaskIdx;
      for (Map.Entry<String, Integer> entry : m.entrySet()) {
        if (taskIndex < entry.getValue()) {
          final String containerType = entry.getKey();
          if (containerType.equals("DRAM")) {
            LOG.info("{} {} index {} {} assigned MemoryStore", containerType, srcTaskId, srcTaskIdx, entry.getValue());
            this.blockStoreValue = DataStoreProperty.Value.MemoryStore;
            break;
          } else {
            LOG.info("{} {} index {} {} assigned LocalFileStore", containerType, srcTaskId, srcTaskIdx, entry.getValue());
            this.blockStoreValue = DataStoreProperty.Value.LocalFileStore;
            break;
          }
        }
      }
    } else {
      LOG.info("{} doesn't have StaticDisaggProp, falling back to {}",
        srcTaskId, runtimeEdge.getPropertyValue(DataStoreProperty.class).get());
      this.blockStoreValue = runtimeEdge.getPropertyValue(DataStoreProperty.class).get();
    }
    
    blockToWrite = blockManagerWorker.createBlock(
        RuntimeIdManager.generateBlockId(runtimeEdge.getId(), srcTaskId), blockStoreValue);
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty =
        runtimeEdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    nonDummyBlock = !duplicateDataProperty.isPresent()
        || duplicateDataProperty.get().getRepresentativeEdgeId().equals(runtimeEdge.getId())
        || duplicateDataProperty.get().getGroupSize() <= 1;
  }

  @Override
  public void write(final Object element) {
    if (nonDummyBlock) {
      blockToWrite.write(partitioner.partition(element), element);

      final DedicatedKeyPerElement dedicatedKeyPerElement =
          partitioner.getClass().getAnnotation(DedicatedKeyPerElement.class);
      if (dedicatedKeyPerElement != null) {
        blockToWrite.commitPartitions();
      }
    } // If else, does not need to write because the data is duplicated.
  }

  @Override
  public void writeWatermark(final Watermark watermark) {
    // do nothing
  }

  /**
   * Notifies that all writes for a block is end.
   * Further write about a committed block will throw an exception.
   */
  @Override
  public void close() {
    // Commit block.
    final DataPersistenceProperty.Value persistence = runtimeEdge
      .getPropertyValue(DataPersistenceProperty.class).get();

    final Optional<Map<Integer, Long>> partitionSizeMap = blockToWrite.commit();
    // Return the total size of the committed block.
    if (partitionSizeMap.isPresent()) {
      long blockSizeTotal = 0;
      for (final Map.Entry<Integer, Long> partitionIdToSize : partitionSizeMap.get().entrySet()) {
        LOG.info("{} partition {} size {}",
          runtimeEdge.getId(), partitionIdToSize.getKey(), partitionIdToSize.getValue());
        blockSizeTotal += partitionIdToSize.getValue();
      }      
      writtenBytes = blockSizeTotal;
    } else {
      writtenBytes = -1; // no written bytes info.
    }
    blockManagerWorker.writeBlock(blockToWrite, blockStoreValue, getExpectedRead(), persistence);
  }

  public Optional<Long> getWrittenBytes() {
    if (writtenBytes == -1) {
      return Optional.empty();
    } else {
      return Optional.of(writtenBytes);
    }
  }

  /**
   * Get the expected number of data read according to the communication pattern of the edge and
   * the parallelism of destination vertex.
   *
   * @return the expected number of data read.
   */
  private int getExpectedRead() {
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty =
      runtimeEdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    final int duplicatedDataMultiplier =
      duplicateDataProperty.isPresent() ? duplicateDataProperty.get().getGroupSize() : 1;
    final int readForABlock = CommunicationPatternProperty.Value.OneToOne.equals(
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class).orElseThrow(
        () -> new RuntimeException("No communication pattern on this edge.")))
      ? 1 : dstIrVertex.getPropertyValue(ParallelismProperty.class).orElseThrow(
      () -> new RuntimeException("No parallelism property on the destination vertex."));
    return readForABlock * duplicatedDataMultiplier;
  }
}
