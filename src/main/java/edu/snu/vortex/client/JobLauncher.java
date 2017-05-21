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
package edu.snu.vortex.client;

import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.ncs.NcsMessageEnvironment;
import edu.snu.vortex.runtime.common.message.ncs.NcsParameters;
import edu.snu.vortex.runtime.master.VortexDriver;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.IdentifierFactory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Job launcher.
 */
public final class JobLauncher {
  private static final Logger LOG = Logger.getLogger(JobLauncher.class.getName());

  /**
   * private constructor.
   */
  private JobLauncher() {
  }

  /**
   * Main JobLauncher method.
   * @param args arguments.
   * @throws Exception exception on the way.
   */
  public static void main(final String[] args) throws Exception {
    // Get Job and Driver Confs
    final Configuration jobConf = getJobConf(args);
    final Configuration driverConf = getDriverConf(jobConf);
    final Configuration driverNcsConf = getDriverNcsConf();
    final Configuration driverMessageConfg = getDriverMessageConf();

    // Merge Job and Driver Confs
    final Configuration jobAndDriverConf = Configurations.merge(jobConf, driverConf, driverNcsConf, driverMessageConfg);

    // Get Runtime Conf
    // TODO #212: Run Vortex on YARN
    final Configuration runtimeConf = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, 20)
        .build();

    // Launch and wait indefinitely for the job to finish
    final LauncherStatus launcherStatus =  DriverLauncher.getLauncher(runtimeConf).run(jobAndDriverConf);
    final Optional<Throwable> possibleError = launcherStatus.getError();
    if (possibleError.isPresent()) {
      throw new RuntimeException(possibleError.get());
    } else {
      LOG.log(Level.INFO, "Job successfully completed (at least it seems...)");
    }
  }

  private static Configuration getDriverNcsConf() throws InjectionException {
    return Configurations.merge(NameServerConfiguration.CONF.build(),
        LocalNameResolverConfiguration.CONF.build(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
            .build());
  }

  private static Configuration getDriverMessageConf() throws InjectionException {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MessageEnvironment.class, NcsMessageEnvironment.class)
        .bindNamedParameter(NcsParameters.SenderId.class, MessageEnvironment.MASTER_COMMUNICATION_ID)
        .build();
  }

  private static Configuration getDriverConf(final Configuration jobConf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(jobConf);
    final String jobId = injector.getNamedInstance(JobConf.JobId.class);
    final int driverMemory = injector.getNamedInstance(JobConf.DriverMemMb.class);
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(VortexDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, VortexDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, VortexDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, VortexDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, VortexDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, VortexDriver.DriverStopHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobId)
        .set(DriverConfiguration.DRIVER_MEMORY, driverMemory)
        .build();
  }

  public static Configuration getJobConf(final String[] args) throws IOException, InjectionException {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(JobConf.JobId.class);
    cl.registerShortNameOfClass(JobConf.UserMainClass.class);
    cl.registerShortNameOfClass(JobConf.UserMainArguments.class);
    cl.registerShortNameOfClass(JobConf.DAGDirectory.class);
    cl.registerShortNameOfClass(JobConf.OptimizationPolicy.class);
    cl.registerShortNameOfClass(JobConf.DriverMemMb.class);
    cl.registerShortNameOfClass(JobConf.ExecutorCores.class);
    cl.registerShortNameOfClass(JobConf.ExecutorMemMb.class);
    cl.registerShortNameOfClass(JobConf.ExecutorNum.class);
    cl.registerShortNameOfClass(JobConf.ExecutorCapacity.class);
    cl.registerShortNameOfClass(JobConf.SchedulerTimeoutMs.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

}
