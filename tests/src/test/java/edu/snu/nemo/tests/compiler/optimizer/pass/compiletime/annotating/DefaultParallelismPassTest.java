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
package edu.snu.nemo.tests.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.SourceVertex;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
import edu.snu.nemo.tests.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link DefaultParallelismPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DefaultParallelismPassTest {
  DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testAnnotatingPass() {
    final AnnotatingPass parallelismPass = new DefaultParallelismPass();
    assertEquals(ExecutionProperty.Key.Parallelism, parallelismPass.getExecutionPropertyToModify());
  }

  @Test
  public void testParallelismOne() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG = new DefaultParallelismPass().apply(compiledDAG);

    processedDAG.getTopologicalSort().forEach(irVertex ->
        assertEquals(1, irVertex.<Integer>getProperty(ExecutionProperty.Key.Parallelism).longValue()));
  }

  @Test
  public void testParallelismTen() throws Exception {
    final int desiredSourceParallelism = 10;
    final DAG<IRVertex, IREdge> processedDAG = new DefaultParallelismPass(desiredSourceParallelism, 2).apply(compiledDAG);

    processedDAG.getTopologicalSort().stream()
        .filter(irVertex -> irVertex instanceof SourceVertex)
        .forEach(irVertex -> assertEquals(desiredSourceParallelism,
            irVertex.<Integer>getProperty(ExecutionProperty.Key.Parallelism).longValue()));
  }
}
