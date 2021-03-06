/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.nemo.runtime.master.resource;

import com.google.protobuf.ByteString;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageSender;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.driver.context.ActiveContext;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * (WARNING) This class is not thread-safe.
 *
 * Contains information/state regarding an executor.
 * Such information may include:
 *    a) The executor's resource type.
 *    b) The executor's capacity (ex. number of cores).
 *    c) Task groups scheduled/launched for the executor.
 *    d) Name of the physical node which hosts this executor.
 *    e) (Please add other information as we implement more features).
 */
@NotThreadSafe
public final class ExecutorRepresenter {

  private final String executorId;
  private final ResourceSpecification resourceSpecification;
  private final Set<String> runningTaskGroups;
  private final Set<String> completeTaskGroups;
  private final Set<String> failedTaskGroups;
  private final MessageSender<ControlMessage.Message> messageSender;
  private final ActiveContext activeContext;
  private final ExecutorService serializationExecutorService;
  private final String nodeName;

  /**
   * Creates a reference to the specified executor.
   * @param executorId the executor id
   * @param resourceSpecification specification for the executor
   * @param messageSender provides communication context for this executor
   * @param activeContext context on the corresponding REEF evaluator
   * @param serializationExecutorService provides threads for message serialization
   * @param nodeName physical name of the node where this executor resides
   */
  public ExecutorRepresenter(final String executorId,
                             final ResourceSpecification resourceSpecification,
                             final MessageSender<ControlMessage.Message> messageSender,
                             final ActiveContext activeContext,
                             final ExecutorService serializationExecutorService,
                             final String nodeName) {
    this.executorId = executorId;
    this.resourceSpecification = resourceSpecification;
    this.messageSender = messageSender;
    this.runningTaskGroups = new HashSet<>();
    this.completeTaskGroups = new HashSet<>();
    this.failedTaskGroups = new HashSet<>();
    this.activeContext = activeContext;
    this.serializationExecutorService = serializationExecutorService;
    this.nodeName = nodeName;
  }

  /**
   * Marks all TaskGroups which were running in this executor as failed.
   */
  public void onExecutorFailed() {
    runningTaskGroups.forEach(taskGroupId -> failedTaskGroups.add(taskGroupId));
    runningTaskGroups.clear();
  }

  /**
   * Marks the TaskGroup as running, and sends scheduling message to the executor.
   * @param scheduledTaskGroup
   */
  public void onTaskGroupScheduled(final ScheduledTaskGroup scheduledTaskGroup) {
    runningTaskGroups.add(scheduledTaskGroup.getTaskGroupId());
    failedTaskGroups.remove(scheduledTaskGroup.getTaskGroupId());

    serializationExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        final byte[] serialized = SerializationUtils.serialize(scheduledTaskGroup);
        sendControlMessage(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.ScheduleTaskGroup)
                .setScheduleTaskGroupMsg(
                    ControlMessage.ScheduleTaskGroupMsg.newBuilder()
                        .setTaskGroup(ByteString.copyFrom(serialized))
                        .build())
                .build());
      }
    });
  }

  /**
   * Sends control message to the executor.
   * @param message Message object to send
   */
  public void sendControlMessage(final ControlMessage.Message message) {
    messageSender.send(message);
  }

  /**
   * Marks the specified TaskGroup as completed.
   * @param taskGroupId id of the TaskGroup
   */
  public void onTaskGroupExecutionComplete(final String taskGroupId) {
    runningTaskGroups.remove(taskGroupId);
    completeTaskGroups.add(taskGroupId);
  }

  /**
   * Marks the specified TaskGroup as failed.
   * @param taskGroupId id of the TaskGroup
   */
  public void onTaskGroupExecutionFailed(final String taskGroupId) {
    runningTaskGroups.remove(taskGroupId);
    failedTaskGroups.add(taskGroupId);
  }

  /**
   * @return how many TaskGroups can this executor simultaneously run
   */
  public int getExecutorCapacity() {
    return resourceSpecification.getCapacity();
  }

  /**
   * @return set of ids of TaskGroups that are running in this executor
   */
  public Set<String> getRunningTaskGroups() {
    return runningTaskGroups;
  }

  /**
   * @return set of ids of TaskGroups that have been failed in this exeuctor
   */
  public Set<String> getFailedTaskGroups() {
    return failedTaskGroups;
  }

  /**
   * @return set of ids of TaskGroups that have been completed in this executor
   */
  public Set<String> getCompleteTaskGroups() {
    return completeTaskGroups;
  }

  /**
   * @return the executor id
   */
  public String getExecutorId() {
    return executorId;
  }

  /**
   * @return the container type
   */
  public String getContainerType() {
    return resourceSpecification.getContainerType();
  }

  /**
   * @return physical name of the node where this executor resides
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Shuts down this executor.
   */
  public void shutDown() {
    activeContext.close();
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("ExecutorRepresenter{");
    sb.append("executorId='").append(executorId).append('\'');
    sb.append(", runningTaskGroups=").append(runningTaskGroups);
    sb.append(", failedTaskGroups=").append(failedTaskGroups);
    sb.append('}');
    return sb.toString();
  }
}

