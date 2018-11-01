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
package org.apache.nemo.examples.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.compiler.optimizer.policy.ConditionalLargeShufflePolicy;
import org.apache.nemo.compiler.optimizer.policy.LargeShufflePolicy;
import org.apache.nemo.examples.beam.policy.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test WordCount program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class WordCountITCase {
  private static final int TIMEOUT = 120000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";

  private static final String inputFileName = "test_input_wordcount";
  private static final String outputFileName = "test_output_wordcount";
  private static final String expectedOutputFileName = "expected_output_wordcount";
  private static final String executorResourceFileName = fileBasePath + "beam_test_executor_resources.json";
  private static final String oneExecutorResourceFileName = fileBasePath + "beam_test_one_executor_resources.json";
  private static final String inputFilePath =  fileBasePath + inputFileName;
  private static final String outputFilePath =  fileBasePath + outputFileName;

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addUserMain(WordCount.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath);
  }

  @After
  public void tearDown() throws Exception {
    try {
      ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName, expectedOutputFileName);
    } finally {
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    }
  }

  @Test (timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
        .addResourceJson(executorResourceFileName)
        .addJobId(WordCountITCase.class.getSimpleName())
        .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testLargeShuffle() throws Exception {
    JobLauncher.main(builder
        .addResourceJson(executorResourceFileName)
        .addJobId(WordCountITCase.class.getSimpleName() + "_largeShuffle")
        //.addOptimizationPolicy(LargeShufflePolicyParallelismFive.class.getCanonicalName())
        .addOptimizationPolicy(LargeShufflePolicy.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testLargeShuffleInOneExecutor() throws Exception {
    JobLauncher.main(builder
        .addResourceJson(oneExecutorResourceFileName)
        .addJobId(WordCountITCase.class.getSimpleName() + "_largeshuffleInOneExecutor")
        .addOptimizationPolicy(LargeShufflePolicyParallelismFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testConditionalLargeShuffle() throws Exception {
    JobLauncher.main(builder
        .addResourceJson(executorResourceFileName)
        .addJobId(WordCountITCase.class.getSimpleName() + "_conditionalLargeShuffle")
        .addOptimizationPolicy(ConditionalLargeShufflePolicy.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testTransientResource() throws Exception {
    JobLauncher.main(builder
        .addResourceJson(executorResourceFileName)
        .addJobId(WordCountITCase.class.getSimpleName() + "_transient")
        .addOptimizationPolicy(TransientResourcePolicyParallelismFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testClonedScheduling() throws Exception {
    JobLauncher.main(builder
        .addResourceJson(executorResourceFileName)
        .addJobId(WordCountITCase.class.getSimpleName() + "_clonedscheduling")
        .addMaxTaskAttempt(Integer.MAX_VALUE)
        .addOptimizationPolicy(UpfrontSchedulingPolicyParallelismFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testSpeculativeExecution() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(WordCountITCase.class.getSimpleName() + "_speculative")
      .addMaxTaskAttempt(Integer.MAX_VALUE)
      .addOptimizationPolicy(AggressiveSpeculativeCloningPolicyParallelismFive.class.getCanonicalName())
      .build());
  }
}
