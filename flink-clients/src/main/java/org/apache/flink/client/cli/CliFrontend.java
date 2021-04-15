/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.cli;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.ProgramMissingJobException;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.client.cli.CliFrontendParser.HELP_OPTION;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of a simple command line frontend for executing programs.
 */
public class CliFrontend {

	private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);

	// actions
	private static final String ACTION_RUN = "run";
	private static final String ACTION_RUN_APPLICATION = "run-application";
	private static final String ACTION_INFO = "info";
	private static final String ACTION_LIST = "list";
	private static final String ACTION_CANCEL = "cancel";
	private static final String ACTION_STOP = "stop";
	private static final String ACTION_SAVEPOINT = "savepoint";

	// configuration dir parameters
	private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
	private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";

	// --------------------------------------------------------------------------------------------

	private final Configuration configuration;

	private final List<CustomCommandLine> customCommandLines;

	private final Options customCommandLineOptions;

	private final Duration clientTimeout;

	private final int defaultParallelism;

	private final ClusterClientServiceLoader clusterClientServiceLoader;

	public CliFrontend(
			Configuration configuration,
			List<CustomCommandLine> customCommandLines) {
		this(configuration, new DefaultClusterClientServiceLoader(), customCommandLines);
	}

	public CliFrontend(
			Configuration configuration,
			ClusterClientServiceLoader clusterClientServiceLoader,
			List<CustomCommandLine> customCommandLines) {
		this.configuration = checkNotNull(configuration);
		this.customCommandLines = checkNotNull(customCommandLines);
		this.clusterClientServiceLoader = checkNotNull(clusterClientServiceLoader);

		FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

		this.customCommandLineOptions = new Options();

		for (CustomCommandLine customCommandLine : customCommandLines) {
			customCommandLine.addGeneralOptions(customCommandLineOptions);
			customCommandLine.addRunOptions(customCommandLineOptions);
		}

		this.clientTimeout = configuration.get(ClientOptions.CLIENT_TIMEOUT);
		this.defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
	}

	// --------------------------------------------------------------------------------------------
	//  Getter & Setter
	// --------------------------------------------------------------------------------------------

	/**
	 * Getter which returns a copy of the associated configuration.
	 *
	 * @return Copy of the associated configuration
	 */
	public Configuration getConfiguration() {
		Configuration copiedConfiguration = new Configuration();

		copiedConfiguration.addAll(configuration);

		return copiedConfiguration;
	}

	public Options getCustomCommandLineOptions() {
		return customCommandLineOptions;
	}

	// --------------------------------------------------------------------------------------------
	//  Execute Actions
	// --------------------------------------------------------------------------------------------

	protected void runApplication(String[] args) throws Exception {
		LOG.info("Running 'run-application' command.");

		final Options commandOptions = CliFrontendParser.getRunCommandOptions();
		final CommandLine commandLine = getCommandLine(commandOptions, args, true);

		if (commandLine.hasOption(HELP_OPTION.getOpt())) {
			CliFrontendParser.printHelpForRunApplication(customCommandLines);
			return;
		}

		final CustomCommandLine activeCommandLine =
				validateAndGetActiveCommandLine(checkNotNull(commandLine));

		final ApplicationDeployer deployer =
			new ApplicationClusterDeployer(clusterClientServiceLoader);

		final ProgramOptions programOptions;
		final Configuration effectiveConfiguration;

		// No need to set a jarFile path for Pyflink job.
		if (ProgramOptionsUtils.isPythonEntryPoint(commandLine)) {
			programOptions = ProgramOptionsUtils.createPythonProgramOptions(commandLine);
			effectiveConfiguration = getEffectiveConfiguration(
				activeCommandLine, commandLine, programOptions, Collections.emptyList());
		} else {
			programOptions = new ProgramOptions(commandLine);
			programOptions.validate();
			final URI uri = PackagedProgramUtils.resolveURI(programOptions.getJarFilePath());
			effectiveConfiguration = getEffectiveConfiguration(
				activeCommandLine, commandLine, programOptions, Collections.singletonList(uri.toString()));
		}

		final ApplicationConfiguration applicationConfiguration =
				new ApplicationConfiguration(programOptions.getProgramArgs(), programOptions.getEntryPointClassName());
		deployer.run(effectiveConfiguration, applicationConfiguration);
	}

	/**
	 * Executions the run action.
	 * shell 提交命令到这个方法
	 * shell：
	 * 	exec
	 * 		$JAVA_RUN
	 * 		$JVM_ARGS
	 * 		$FLINK_ENV_JAVA_OPTS
	 * 		"${log_setting[@]}"
	 * 		-classpath
	 * 		"`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`"   constructFlinkClassPath
	 * 		org.apache.flink.client.cli.CliFrontend "$@"
	 *
	 *
	 * 	/opt/jdk1.8/bin/java
	 * -Dlog.file=/opt/flink/log/flink-root-client-dataMiddle-100.log
	 * -Dlog4j.configuration=file:/opt/flink/conf/log4j-cli.properties
	 * -Dlog4j.configurationFile=file:/opt/flink/conf/log4j-cli.properties
	 * -Dlogback.configurationFile=file:/opt/flink/conf/logback.xml
	 * -classpath FlinkClassPath&HadoopClassPath
	 *  org.apache.flink.client.cli.CliFrontend
	 *  run -m yarn-cluster -p 3 -ynm datamiddle-test-submit -c com.xy.core.wc.WordCount -yjm 20m -ytm 50m
	 *  /opt/runJar/Flink_Tutorial-1.0-SNAPSHOT.jar
	 *
	 * @param args Command line arguments for the run action.
	 */
	protected void run(String[] args) throws Exception {
		LOG.info("Running 'run' command.");
		// 获取 Linux shell提交的参数key , -s -m -h -c 等等
		final Options commandOptions = CliFrontendParser.getRunCommandOptions();
		// commandLine ={args,options}
		final CommandLine commandLine = getCommandLine(commandOptions, args, true);

		// 包含help 帮助命令 直接返回
		// evaluate help flag
		if (commandLine.hasOption(HELP_OPTION.getOpt())) {
			CliFrontendParser.printHelpForRun(customCommandLines);
			return;
		}

		// 命令校验，校验命令是否有必要执行
		final CustomCommandLine activeCommandLine =
				validateAndGetActiveCommandLine(checkNotNull(commandLine));

		// 创建程序的Options 参数集 by commandLine
		final ProgramOptions programOptions = ProgramOptions.create(commandLine);

		// 要运行的jar包的URL列表
		final List<URL> jobJars = getJobJarAndDependencies(programOptions);

		// 获取有效配置
		final Configuration effectiveConfiguration = getEffectiveConfiguration(
				activeCommandLine, commandLine, programOptions, jobJars);

		LOG.debug("Effective executor configuration: {}", effectiveConfiguration);
		// 获取程序包
		/***
		 * return PackagedProgram.newBuilder()
		 * 			.setJarFile(jarFile) jar包名称
		 * 			.setUserClassPaths(classpaths) flinkClassPath HadoopClassPath List<URL> classpaths 以冒号分割
		 * 			.setEntryPointClassName(entryPointClass) job主类
		 * 			.setConfiguration(configuration) 有效配置
		 * 			.setSavepointRestoreSettings(runOptions.getSavepointRestoreSettings()) -s  默认NONE
		 * 			.setArguments(programArgs) 参数
		 * 			.build();
		 **/
		final PackagedProgram program = getPackagedProgram(programOptions, effectiveConfiguration);

		try {
			// 执行程序
			executeProgram(effectiveConfiguration, program);
		} finally {
			program.deleteExtractedLibraries();
		}
	}

	/**
	 * Get all provided libraries needed to run the program from the ProgramOptions.
	 */
	private List<URL> getJobJarAndDependencies(ProgramOptions programOptions) throws CliArgsException {
		// class path 运行主类
		String entryPointClass = programOptions.getEntryPointClassName();
		// jar包路径
		String jarFilePath = programOptions.getJarFilePath();

		try {
			File jarFile = jarFilePath != null ? getJarFile(jarFilePath) : null;
			return PackagedProgram.getJobJarAndDependencies(jarFile, entryPointClass);//返回jar包的List<URL>
		} catch (FileNotFoundException | ProgramInvocationException e) {
			throw new CliArgsException("Could not get job jar and dependencies from JAR file: " + e.getMessage(), e);
		}
	}

	private PackagedProgram getPackagedProgram(
			ProgramOptions programOptions,
			Configuration effectiveConfiguration) throws ProgramInvocationException, CliArgsException {
		PackagedProgram program;
		try {
			LOG.info("Building program from JAR file");
			program = buildProgram(programOptions, effectiveConfiguration);
		} catch (FileNotFoundException e) {
			throw new CliArgsException("Could not build the program from JAR file: " + e.getMessage(), e);
		}
		return program;
	}

	private <T> Configuration getEffectiveConfiguration(
			final CustomCommandLine activeCustomCommandLine,
			final CommandLine commandLine) throws FlinkException {

		final Configuration effectiveConfiguration = new Configuration(configuration);

		final Configuration commandLineConfiguration =
				checkNotNull(activeCustomCommandLine).toConfiguration(commandLine);

		effectiveConfiguration.addAll(commandLineConfiguration);

		return effectiveConfiguration;
	}

	private <T> Configuration getEffectiveConfiguration(
			final CustomCommandLine activeCustomCommandLine,
			final CommandLine commandLine,
			final ProgramOptions programOptions,
			final List<T> jobJars) throws FlinkException {

		final Configuration effectiveConfiguration = getEffectiveConfiguration(activeCustomCommandLine, commandLine);

		final ExecutionConfigAccessor executionParameters = ExecutionConfigAccessor.fromProgramOptions(
				checkNotNull(programOptions),
				checkNotNull(jobJars));

		executionParameters.applyToConfiguration(effectiveConfiguration);

		LOG.debug("Effective configuration after Flink conf, custom commandline, and program options: {}", effectiveConfiguration);
		return effectiveConfiguration;
	}

	/**
	 * Executes the info action.
	 *
	 * @param args Command line arguments for the info action.
	 */
	protected void info(String[] args) throws Exception {
		LOG.info("Running 'info' command.");

		final Options commandOptions = CliFrontendParser.getInfoCommandOptions();

		final CommandLine commandLine = CliFrontendParser.parse(commandOptions, args, true);

		final ProgramOptions programOptions = ProgramOptions.create(commandLine);

		// evaluate help flag
		if (commandLine.hasOption(HELP_OPTION.getOpt())) {
			CliFrontendParser.printHelpForInfo();
			return;
		}

		// -------- build the packaged program -------------

		LOG.info("Building program from JAR file");

		PackagedProgram program = null;

		try {
			int parallelism = programOptions.getParallelism();
			if (ExecutionConfig.PARALLELISM_DEFAULT == parallelism) {
				parallelism = defaultParallelism;
			}

			LOG.info("Creating program plan dump");

			final CustomCommandLine activeCommandLine =
					validateAndGetActiveCommandLine(checkNotNull(commandLine));

			final Configuration effectiveConfiguration = getEffectiveConfiguration(
					activeCommandLine, commandLine, programOptions, getJobJarAndDependencies(programOptions));

			program = buildProgram(programOptions, effectiveConfiguration);

			Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(program, effectiveConfiguration, parallelism, true);
			String jsonPlan = FlinkPipelineTranslationUtil.translateToJSONExecutionPlan(pipeline);

			if (jsonPlan != null) {
				System.out.println("----------------------- Execution Plan -----------------------");
				System.out.println(jsonPlan);
				System.out.println("--------------------------------------------------------------");
			}
			else {
				System.out.println("JSON plan could not be generated.");
			}

			String description = program.getDescription();
			if (description != null) {
				System.out.println();
				System.out.println(description);
			}
			else {
				System.out.println();
				System.out.println("No description provided.");
			}
		}
		finally {
			if (program != null) {
				program.deleteExtractedLibraries();
			}
		}
	}

	/**
	 * Executes the list action.
	 *
	 * @param args Command line arguments for the list action.
	 */
	protected void list(String[] args) throws Exception {
		LOG.info("Running 'list' command.");

		final Options commandOptions = CliFrontendParser.getListCommandOptions();
		final CommandLine commandLine = getCommandLine(commandOptions, args, false);

		ListOptions listOptions = new ListOptions(commandLine);

		// evaluate help flag
		if (listOptions.isPrintHelp()) {
			CliFrontendParser.printHelpForList(customCommandLines);
			return;
		}

		final boolean showRunning;
		final boolean showScheduled;
		final boolean showAll;

		// print running and scheduled jobs if not option supplied
		if (!listOptions.showRunning() && !listOptions.showScheduled() && !listOptions.showAll()) {
			showRunning = true;
			showScheduled = true;
			showAll = false;
		} else {
			showRunning = listOptions.showRunning();
			showScheduled = listOptions.showScheduled();
			showAll = listOptions.showAll();
		}

		final CustomCommandLine activeCommandLine = validateAndGetActiveCommandLine(commandLine);

		runClusterAction(
			activeCommandLine,
			commandLine,
			clusterClient -> listJobs(clusterClient, showRunning, showScheduled, showAll));

	}

	private <ClusterID> void listJobs(
			ClusterClient<ClusterID> clusterClient,
			boolean showRunning,
			boolean showScheduled,
			boolean showAll) throws FlinkException {
		Collection<JobStatusMessage> jobDetails;
		try {
			CompletableFuture<Collection<JobStatusMessage>> jobDetailsFuture = clusterClient.listJobs();

			logAndSysout("Waiting for response...");
			jobDetails = jobDetailsFuture.get();

		} catch (Exception e) {
			Throwable cause = ExceptionUtils.stripExecutionException(e);
			throw new FlinkException("Failed to retrieve job list.", cause);
		}

		LOG.info("Successfully retrieved list of jobs");

		final List<JobStatusMessage> runningJobs = new ArrayList<>();
		final List<JobStatusMessage> scheduledJobs = new ArrayList<>();
		final List<JobStatusMessage> terminatedJobs = new ArrayList<>();
		jobDetails.forEach(details -> {
			if (details.getJobState() == JobStatus.CREATED || details.getJobState() == JobStatus.INITIALIZING) {
				scheduledJobs.add(details);
			} else if (!details.getJobState().isGloballyTerminalState()) {
				runningJobs.add(details);
			} else {
				terminatedJobs.add(details);
			}
		});

		if (showRunning || showAll) {
			if (runningJobs.size() == 0) {
				System.out.println("No running jobs.");
			}
			else {
				System.out.println("------------------ Running/Restarting Jobs -------------------");
				printJobStatusMessages(runningJobs);
				System.out.println("--------------------------------------------------------------");
			}
		}
		if (showScheduled || showAll) {
			if (scheduledJobs.size() == 0) {
				System.out.println("No scheduled jobs.");
			}
			else {
				System.out.println("----------------------- Scheduled Jobs -----------------------");
				printJobStatusMessages(scheduledJobs);
				System.out.println("--------------------------------------------------------------");
			}
		}
		if (showAll) {
			if (terminatedJobs.size() != 0) {
				System.out.println("---------------------- Terminated Jobs -----------------------");
				printJobStatusMessages(terminatedJobs);
				System.out.println("--------------------------------------------------------------");
			}
		}
	}

	private static void printJobStatusMessages(List<JobStatusMessage> jobs) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
		Comparator<JobStatusMessage> startTimeComparator = (o1, o2) -> (int) (o1.getStartTime() - o2.getStartTime());
		Comparator<Map.Entry<JobStatus, List<JobStatusMessage>>> statusComparator =
			(o1, o2) -> String.CASE_INSENSITIVE_ORDER.compare(o1.getKey().toString(), o2.getKey().toString());

		Map<JobStatus, List<JobStatusMessage>> jobsByState = jobs.stream().collect(Collectors.groupingBy(JobStatusMessage::getJobState));
		jobsByState.entrySet().stream()
			.sorted(statusComparator)
			.map(Map.Entry::getValue).flatMap(List::stream).sorted(startTimeComparator)
			.forEachOrdered(job ->
				System.out.println(dateFormat.format(new Date(job.getStartTime()))
					+ " : " + job.getJobId() + " : " + job.getJobName()
					+ " (" + job.getJobState() + ")"));
	}

	/**
	 * Executes the STOP action.
	 *
	 * @param args Command line arguments for the stop action.
	 */
	protected void stop(String[] args) throws Exception {
		LOG.info("Running 'stop-with-savepoint' command.");

		final Options commandOptions = CliFrontendParser.getStopCommandOptions();
		final CommandLine commandLine = getCommandLine(commandOptions, args, false);

		final StopOptions stopOptions = new StopOptions(commandLine);
		if (stopOptions.isPrintHelp()) {
			CliFrontendParser.printHelpForStop(customCommandLines);
			return;
		}

		final String[] cleanedArgs = stopOptions.getArgs();

		final String targetDirectory = stopOptions.hasSavepointFlag() && cleanedArgs.length > 0
				? stopOptions.getTargetDirectory()
				: null; // the default savepoint location is going to be used in this case.

		final JobID jobId = cleanedArgs.length != 0
				? parseJobId(cleanedArgs[0])
				: parseJobId(stopOptions.getTargetDirectory());

		final boolean advanceToEndOfEventTime = stopOptions.shouldAdvanceToEndOfEventTime();

		logAndSysout((advanceToEndOfEventTime ? "Draining job " : "Suspending job ") + "\"" + jobId + "\" with a savepoint.");

		final CustomCommandLine activeCommandLine = validateAndGetActiveCommandLine(commandLine);
		runClusterAction(
			activeCommandLine,
			commandLine,
			clusterClient -> {
				final String savepointPath;
				try {
					savepointPath = clusterClient.stopWithSavepoint(jobId, advanceToEndOfEventTime, targetDirectory).get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
				} catch (Exception e) {
					throw new FlinkException("Could not stop with a savepoint job \"" + jobId + "\".", e);
				}
				logAndSysout("Savepoint completed. Path: " + savepointPath);
			});
	}

	/**
	 * Executes the CANCEL action.
	 *
	 * @param args Command line arguments for the cancel action.
	 */
	protected void cancel(String[] args) throws Exception {
		LOG.info("Running 'cancel' command.");

		final Options commandOptions = CliFrontendParser.getCancelCommandOptions();
		final CommandLine commandLine = getCommandLine(commandOptions, args, false);

		CancelOptions cancelOptions = new CancelOptions(commandLine);

		// evaluate help flag
		if (cancelOptions.isPrintHelp()) {
			CliFrontendParser.printHelpForCancel(customCommandLines);
			return;
		}

		final CustomCommandLine activeCommandLine = validateAndGetActiveCommandLine(commandLine);

		final String[] cleanedArgs = cancelOptions.getArgs();

		if (cancelOptions.isWithSavepoint()) {

			logAndSysout("DEPRECATION WARNING: Cancelling a job with savepoint is deprecated. Use \"stop\" instead.");

			final JobID jobId;
			final String targetDirectory;

			if (cleanedArgs.length > 0) {
				jobId = parseJobId(cleanedArgs[0]);
				targetDirectory = cancelOptions.getSavepointTargetDirectory();
			} else {
				jobId = parseJobId(cancelOptions.getSavepointTargetDirectory());
				targetDirectory = null;
			}

			if (targetDirectory == null) {
				logAndSysout("Cancelling job " + jobId + " with savepoint to default savepoint directory.");
			} else {
				logAndSysout("Cancelling job " + jobId + " with savepoint to " + targetDirectory + '.');
			}

			runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> {
					final String savepointPath;
					try {
						savepointPath = clusterClient.cancelWithSavepoint(jobId, targetDirectory).get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
					} catch (Exception e) {
						throw new FlinkException("Could not cancel job " + jobId + '.', e);
					}
					logAndSysout("Cancelled job " + jobId + ". Savepoint stored in " + savepointPath + '.');
				});
		} else {
			final JobID jobId;

			if (cleanedArgs.length > 0) {
				jobId = parseJobId(cleanedArgs[0]);
			} else {
				throw new CliArgsException("Missing JobID. Specify a JobID to cancel a job.");
			}

			logAndSysout("Cancelling job " + jobId + '.');

			runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> {
					try {
						clusterClient.cancel(jobId).get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
					} catch (Exception e) {
						throw new FlinkException("Could not cancel job " + jobId + '.', e);
					}
				});

			logAndSysout("Cancelled job " + jobId + '.');
		}
	}


	public CommandLine getCommandLine(final Options commandOptions, final String[] args, final boolean stopAtNonOptions) throws CliArgsException {
		// shell -> run stopAtNonOptions = true
		final Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);
		// return args and options()
		return CliFrontendParser.parse(commandLineOptions, args, stopAtNonOptions);
	}

	/**
	 * Executes the SAVEPOINT action.
	 *
	 * @param args Command line arguments for the savepoint action.
	 */
	protected void savepoint(String[] args) throws Exception {
		LOG.info("Running 'savepoint' command.");

		final Options commandOptions = CliFrontendParser.getSavepointCommandOptions();

		final Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);

		final CommandLine commandLine = CliFrontendParser.parse(commandLineOptions, args, false);

		final SavepointOptions savepointOptions = new SavepointOptions(commandLine);

		// evaluate help flag
		if (savepointOptions.isPrintHelp()) {
			CliFrontendParser.printHelpForSavepoint(customCommandLines);
			return;
		}

		final CustomCommandLine activeCommandLine = validateAndGetActiveCommandLine(commandLine);

		if (savepointOptions.isDispose()) {
			runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> disposeSavepoint(clusterClient, savepointOptions.getSavepointPath()));
		} else {
			String[] cleanedArgs = savepointOptions.getArgs();

			final JobID jobId;

			if (cleanedArgs.length >= 1) {
				String jobIdString = cleanedArgs[0];

				jobId = parseJobId(jobIdString);
			} else {
				throw new CliArgsException("Missing JobID. " +
					"Specify a Job ID to trigger a savepoint.");
			}

			final String savepointDirectory;
			if (cleanedArgs.length >= 2) {
				savepointDirectory = cleanedArgs[1];
			} else {
				savepointDirectory = null;
			}

			// Print superfluous arguments
			if (cleanedArgs.length >= 3) {
				logAndSysout("Provided more arguments than required. Ignoring not needed arguments.");
			}

			runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> triggerSavepoint(clusterClient, jobId, savepointDirectory));
		}

	}

	/**
	 * Sends a SavepointTriggerMessage to the job manager.
	 */
	private void triggerSavepoint(ClusterClient<?> clusterClient, JobID jobId, String savepointDirectory) throws FlinkException {
		logAndSysout("Triggering savepoint for job " + jobId + '.');

		CompletableFuture<String> savepointPathFuture = clusterClient.triggerSavepoint(jobId, savepointDirectory);

		logAndSysout("Waiting for response...");

		try {
			final String savepointPath = savepointPathFuture.get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);

			logAndSysout("Savepoint completed. Path: " + savepointPath);
			logAndSysout("You can resume your program from this savepoint with the run command.");
		} catch (Exception e) {
			Throwable cause = ExceptionUtils.stripExecutionException(e);
			throw new FlinkException("Triggering a savepoint for the job " + jobId + " failed.", cause);
		}
	}

	/**
	 * Sends a SavepointDisposalRequest to the job manager.
	 */
	private void disposeSavepoint(ClusterClient<?> clusterClient, String savepointPath) throws FlinkException {
		checkNotNull(savepointPath, "Missing required argument: savepoint path. " +
			"Usage: bin/flink savepoint -d <savepoint-path>");

		logAndSysout("Disposing savepoint '" + savepointPath + "'.");

		final CompletableFuture<Acknowledge> disposeFuture = clusterClient.disposeSavepoint(savepointPath);

		logAndSysout("Waiting for response...");

		try {
			disposeFuture.get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			throw new FlinkException("Disposing the savepoint '" + savepointPath + "' failed.", e);
		}

		logAndSysout("Savepoint '" + savepointPath + "' disposed.");
	}

	// --------------------------------------------------------------------------------------------
	//  Interaction with programs and JobManager
	// --------------------------------------------------------------------------------------------

	protected void executeProgram(final Configuration configuration, final PackagedProgram program) throws ProgramInvocationException {
		ClientUtils.executeProgram(new DefaultExecutorServiceLoader(), configuration, program, false, false);
	}

	/**
	 * Creates a Packaged program from the given command line options.
	 *
	 * @return A PackagedProgram (upon success)
	 */
	PackagedProgram buildProgram(final ProgramOptions runOptions)
			throws FileNotFoundException, ProgramInvocationException, CliArgsException {
		return buildProgram(runOptions, configuration);
	}

	/**
	 * Creates a Packaged program from the given command line options and the effectiveConfiguration.
	 *
	 * @return A PackagedProgram (upon success)
	 */
	PackagedProgram buildProgram(final ProgramOptions runOptions, final Configuration configuration) throws FileNotFoundException, ProgramInvocationException, CliArgsException {
		runOptions.validate();

		String[] programArgs = runOptions.getProgramArgs();
		String jarFilePath = runOptions.getJarFilePath();
		List<URL> classpaths = runOptions.getClasspaths();

		// Get assembler class
		String entryPointClass = runOptions.getEntryPointClassName();
		File jarFile = jarFilePath != null ? getJarFile(jarFilePath) : null;

		return PackagedProgram.newBuilder()
			.setJarFile(jarFile)
			.setUserClassPaths(classpaths)
			.setEntryPointClassName(entryPointClass)
			.setConfiguration(configuration)
			.setSavepointRestoreSettings(runOptions.getSavepointRestoreSettings())
			.setArguments(programArgs)
			.build();
	}

	/**
	 * Gets the JAR file from the path.
	 *
	 * @param jarFilePath The path of JAR file
	 * @return The JAR file
	 * @throws FileNotFoundException The JAR file does not exist.
	 */
	private File getJarFile(String jarFilePath) throws FileNotFoundException {
		File jarFile = new File(jarFilePath);
		// Check if JAR file exists
		if (!jarFile.exists()) {
			throw new FileNotFoundException("JAR file does not exist: " + jarFile);
		}
		else if (!jarFile.isFile()) {
			throw new FileNotFoundException("JAR file is not a file: " + jarFile);
		}
		return jarFile;
	}

	// --------------------------------------------------------------------------------------------
	//  Logging and Exception Handling
	// --------------------------------------------------------------------------------------------

	/**
	 * Displays an exception message for incorrect command line arguments.
	 *
	 * @param e The exception to display.
	 * @return The return code for the process.
	 */
	private static int handleArgException(CliArgsException e) {
		LOG.error("Invalid command line arguments.", e);

		System.out.println(e.getMessage());
		System.out.println();
		System.out.println("Use the help option (-h or --help) to get help on the command.");
		return 1;
	}

	/**
	 * Displays an optional exception message for incorrect program parametrization.
	 *
	 * @param e The exception to display.
	 * @return The return code for the process.
	 */
	private static int handleParametrizationException(ProgramParametrizationException e) {
		LOG.error("Program has not been parametrized properly.", e);
		System.err.println(e.getMessage());
		return 1;
	}

	/**
	 * Displays a message for a program without a job to execute.
	 *
	 * @return The return code for the process.
	 */
	private static int handleMissingJobException() {
		System.err.println();
		System.err.println("The program didn't contain a Flink job. " +
			"Perhaps you forgot to call execute() on the execution environment.");
		return 1;
	}

	/**
	 * Displays an exception message.
	 *
	 * @param t The exception to display.
	 * @return The return code for the process.
	 */
	private static int handleError(Throwable t) {
		LOG.error("Error while running the command.", t);

		System.err.println();
		System.err.println("------------------------------------------------------------");
		System.err.println(" The program finished with the following exception:");
		System.err.println();

		if (t.getCause() instanceof InvalidProgramException) {
			System.err.println(t.getCause().getMessage());
			StackTraceElement[] trace = t.getCause().getStackTrace();
			for (StackTraceElement ele: trace) {
				System.err.println("\t" + ele);
				if (ele.getMethodName().equals("main")) {
					break;
				}
			}
		} else {
			t.printStackTrace();
		}
		return 1;
	}

	private static void logAndSysout(String message) {
		LOG.info(message);
		System.out.println(message);
	}

	// --------------------------------------------------------------------------------------------
	//  Internal methods
	// --------------------------------------------------------------------------------------------

	private JobID parseJobId(String jobIdString) throws CliArgsException {
		if (jobIdString == null) {
			throw new CliArgsException("Missing JobId");
		}

		final JobID jobId;
		try {
			jobId = JobID.fromHexString(jobIdString);
		} catch (IllegalArgumentException e) {
			throw new CliArgsException(e.getMessage());
		}
		return jobId;
	}

	/**
	 * Retrieves the {@link ClusterClient} from the given {@link CustomCommandLine} and runs the given
	 * {@link ClusterAction} against it.
	 *
	 * @param activeCommandLine to create the {@link ClusterDescriptor} from
	 * @param commandLine containing the parsed command line options
	 * @param clusterAction the cluster action to run against the retrieved {@link ClusterClient}.
	 * @param <ClusterID> type of the cluster id
	 * @throws FlinkException if something goes wrong
	 */
	private <ClusterID> void runClusterAction(CustomCommandLine activeCommandLine, CommandLine commandLine, ClusterAction<ClusterID> clusterAction) throws FlinkException {
		final Configuration effectiveConfiguration = getEffectiveConfiguration(activeCommandLine, commandLine);
		LOG.debug("Effective configuration after Flink conf, and custom commandline: {}", effectiveConfiguration);

		final ClusterClientFactory<ClusterID> clusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(effectiveConfiguration);

		final ClusterID clusterId = clusterClientFactory.getClusterId(effectiveConfiguration);
		if (clusterId == null) {
			throw new FlinkException("No cluster id was specified. Please specify a cluster to which you would like to connect.");
		}

		try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(effectiveConfiguration)) {
			try (final ClusterClient<ClusterID> clusterClient = clusterDescriptor.retrieve(clusterId).getClusterClient()) {
				clusterAction.runAction(clusterClient);
			}
		}
	}

	/**
	 * Internal interface to encapsulate cluster actions which are executed via
	 * the {@link ClusterClient}.
	 *
	 * @param <ClusterID> type of the cluster id
	 */
	@FunctionalInterface
	private interface ClusterAction<ClusterID> {

		/**
		 * Run the cluster action with the given {@link ClusterClient}.
		 *
		 * @param clusterClient to run the cluster action against
		 * @throws FlinkException if something goes wrong
		 */
		void runAction(ClusterClient<ClusterID> clusterClient) throws FlinkException;
	}

	// --------------------------------------------------------------------------------------------
	//  Entry point for executable
	// --------------------------------------------------------------------------------------------

	/**
	 * Parses the command line arguments and starts the requested action.
	 *
	 * @param args command line arguments of the client.
	 * @return The return code of the program
	 */
	public int parseAndRun(String[] args) {

		// check for action
		if (args.length < 1) {
			CliFrontendParser.printHelp(customCommandLines);
			System.out.println("Please specify an action.");
			return 1;
		}

		// get action
		// run
		String action = args[0];

		// remove action from parameters
		// 拆分run
		final String[] params = Arrays.copyOfRange(args, 1, args.length);

		try {
			// do action
			switch (action) {
				case ACTION_RUN:
					run(params);
					return 0;
				case ACTION_RUN_APPLICATION:
					runApplication(params);
					return 0;
				case ACTION_LIST:
					list(params);
					return 0;
				case ACTION_INFO:
					info(params);
					return 0;
				case ACTION_CANCEL:
					cancel(params);
					return 0;
				case ACTION_STOP:
					stop(params);
					return 0;
				case ACTION_SAVEPOINT:
					savepoint(params);
					return 0;
				case "-h":
				case "--help":
					CliFrontendParser.printHelp(customCommandLines);
					return 0;
				case "-v":
				case "--version":
					String version = EnvironmentInformation.getVersion();
					String commitID = EnvironmentInformation.getRevisionInformation().commitId;
					System.out.print("Version: " + version);
					System.out.println(commitID.equals(EnvironmentInformation.UNKNOWN) ? "" : ", Commit ID: " + commitID);
					return 0;
				default:
					System.out.printf("\"%s\" is not a valid action.\n", action);
					System.out.println();
					System.out.println("Valid actions are \"run\", \"list\", \"info\", \"savepoint\", \"stop\", or \"cancel\".");
					System.out.println();
					System.out.println("Specify the version option (-v or --version) to print Flink version.");
					System.out.println();
					System.out.println("Specify the help option (-h or --help) to get help on the command.");
					return 1;
			}
		} catch (CliArgsException ce) {
			return handleArgException(ce);
		} catch (ProgramParametrizationException ppe) {
			return handleParametrizationException(ppe);
		} catch (ProgramMissingJobException pmje) {
			return handleMissingJobException();
		} catch (Exception e) {
			return handleError(e);
		}
	}

	/**
	 * Submits the job based on the arguments.
	 */
	public static void main(final String[] args) {
		String argsStr = "/opt/jdk1.8/bin/java -Dlog.file=/opt/flink/log/flink-root-client-dataMiddle-100.log -Dlog4j.configuration=file:/opt/flink/conf/log4j-cli.properties -Dlog4j.configurationFile=file:/opt/flink/conf/log4j-cli.properties -Dlogback.configurationFile=file:/opt/flink/conf/logback.xml -classpath /opt/flink/lib/flink-csv-1.12.0.jar:/opt/flink/lib/flink-engine-lib/dom4j-1.6.1.jar:/opt/flink/lib/flink-engine-lib/flink-connector-jdbc_2.12-1.12.0.jar:/opt/flink/lib/flink-engine-lib/gson-2.8.6.jar:/opt/flink/lib/flink-engine-lib/ojdbc7-12.1.0.2.jar:/opt/flink/lib/flink-json-1.12.0.jar:/opt/flink/lib/flink-shaded-zookeeper-3.4.14.jar:/opt/flink/lib/flink-table_2.12-1.12.0.jar:/opt/flink/lib/flink-table-blink_2.12-1.12.0.jar:/opt/flink/lib/hive-lib/flink-connector-hive_2.12-1.12.0.jar:/opt/flink/lib/hive-lib/hive-jdbc-3.1.2.jar:/opt/flink/lib/hive-lib/libthrift-0.9.3.jar:/opt/flink/lib/kafka-lib/flink-connector-kafka_2.12-1.12.0.jar:/opt/flink/lib/kafka-lib/kafka-clients-2.3.1.jar:/opt/flink/lib/log4j-1.2-api-2.12.1.jar:/opt/flink/lib/log4j-api-2.12.1.jar:/opt/flink/lib/log4j-core-2.12.1.jar:/opt/flink/lib/log4j-slf4j-impl-2.12.1.jar:/opt/flink/lib/flink-dist_2.12-1.12.0.jar:/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/mapreduce/lib/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*:/opt/hadoop/share/hadoop/common/lib/accessors-smart-1.2.jar:/opt/hadoop/share/hadoop/common/lib/animal-sniffer-annotations-1.17.jar:/opt/hadoop/share/hadoop/common/lib/asm-5.0.4.jar:/opt/hadoop/share/hadoop/common/lib/audience-annotations-0.5.0.jar:/opt/hadoop/share/hadoop/common/lib/avro-1.7.7.jar:/opt/hadoop/share/hadoop/common/lib/checker-qual-2.5.2.jar:/opt/hadoop/share/hadoop/common/lib/commons-beanutils-1.9.3.jar:/opt/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar:/opt/hadoop/share/hadoop/common/lib/commons-codec-1.11.jar:/opt/hadoop/share/hadoop/common/lib/commons-collections-3.2.2.jar:/opt/hadoop/share/hadoop/common/lib/commons-compress-1.18.jar:/opt/hadoop/share/hadoop/common/lib/commons-configuration2-2.1.1.jar:/opt/hadoop/share/hadoop/common/lib/commons-io-2.5.jar:/opt/hadoop/share/hadoop/common/lib/commons-lang-2.6.jar:/opt/hadoop/share/hadoop/common/lib/commons-lang3-3.4.jar:/opt/hadoop/share/hadoop/common/lib/commons-logging-1.1.3.jar:/opt/hadoop/share/hadoop/common/lib/commons-math3-3.1.1.jar:/opt/hadoop/share/hadoop/common/lib/commons-net-3.6.jar:/opt/hadoop/share/hadoop/common/lib/curator-client-2.13.0.jar:/opt/hadoop/share/hadoop/common/lib/curator-framework-2.13.0.jar:/opt/hadoop/share/hadoop/common/lib/curator-recipes-2.13.0.jar:/opt/hadoop/share/hadoop/common/lib/error_prone_annotations-2.2.0.jar:/opt/hadoop/share/hadoop/common/lib/failureaccess-1.0.jar:/opt/hadoop/share/hadoop/common/lib/gson-2.2.4.jar:/opt/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar:/opt/hadoop/share/hadoop/common/lib/hadoop-annotations-3.1.3.jar:/opt/hadoop/share/hadoop/common/lib/hadoop-auth-3.1.3.jar:/opt/hadoop/share/hadoop/common/lib/htrace-core4-4.1.0-incubating.jar:/opt/hadoop/share/hadoop/common/lib/httpclient-4.5.2.jar:/opt/hadoop/share/hadoop/common/lib/httpcore-4.4.4.jar:/opt/hadoop/share/hadoop/common/lib/j2objc-annotations-1.1.jar:/opt/hadoop/share/hadoop/common/lib/jackson-annotations-2.7.8.jar:/opt/hadoop/share/hadoop/common/lib/jackson-core-2.7.8.jar:/opt/hadoop/share/hadoop/common/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/common/lib/jackson-databind-2.7.8.jar:/opt/hadoop/share/hadoop/common/lib/jackson-jaxrs-1.9.13.jar:/opt/hadoop/share/hadoop/common/lib/jackson-mapper-asl-1.9.13.jar:/opt/hadoop/share/hadoop/common/lib/jackson-xc-1.9.13.jar:/opt/hadoop/share/hadoop/common/lib/javax.servlet-api-3.1.0.jar:/opt/hadoop/share/hadoop/common/lib/jaxb-api-2.2.11.jar:/opt/hadoop/share/hadoop/common/lib/jaxb-impl-2.2.3-1.jar:/opt/hadoop/share/hadoop/common/lib/jcip-annotations-1.0-1.jar:/opt/hadoop/share/hadoop/common/lib/jersey-core-1.19.jar:/opt/hadoop/share/hadoop/common/lib/jersey-json-1.19.jar:/opt/hadoop/share/hadoop/common/lib/jersey-server-1.19.jar:/opt/hadoop/share/hadoop/common/lib/jersey-servlet-1.19.jar:/opt/hadoop/share/hadoop/common/lib/jettison-1.1.jar:/opt/hadoop/share/hadoop/common/lib/jetty-http-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/common/lib/jetty-io-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/common/lib/jetty-security-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/common/lib/jetty-server-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/common/lib/jetty-servlet-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/common/lib/jetty-util-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/common/lib/jetty-webapp-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/common/lib/jetty-xml-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/common/lib/jsch-0.1.54.jar:/opt/hadoop/share/hadoop/common/lib/json-smart-2.3.jar:/opt/hadoop/share/hadoop/common/lib/jsp-api-2.1.jar:/opt/hadoop/share/hadoop/common/lib/jsr305-3.0.0.jar:/opt/hadoop/share/hadoop/common/lib/jsr311-api-1.1.1.jar:/opt/hadoop/share/hadoop/common/lib/jul-to-slf4j-1.7.25.jar:/opt/hadoop/share/hadoop/common/lib/kerb-admin-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerb-client-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerb-common-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerb-core-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerb-crypto-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerb-identity-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerb-server-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerb-simplekdc-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerb-util-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerby-asn1-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerby-config-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerby-pkix-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerby-util-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/kerby-xdr-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/opt/hadoop/share/hadoop/common/lib/log4j-1.2.17.jar:/opt/hadoop/share/hadoop/common/lib/metrics-core-3.2.4.jar:/opt/hadoop/share/hadoop/common/lib/netty-3.10.5.Final.jar:/opt/hadoop/share/hadoop/common/lib/nimbus-jose-jwt-4.41.1.jar:/opt/hadoop/share/hadoop/common/lib/paranamer-2.3.jar:/opt/hadoop/share/hadoop/common/lib/protobuf-java-2.5.0.jar:/opt/hadoop/share/hadoop/common/lib/re2j-1.1.jar:/opt/hadoop/share/hadoop/common/lib/slf4j-api-1.7.25.jar:/opt/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar:/opt/hadoop/share/hadoop/common/lib/snappy-java-1.0.5.jar:/opt/hadoop/share/hadoop/common/lib/stax2-api-3.1.4.jar:/opt/hadoop/share/hadoop/common/lib/token-provider-1.0.1.jar:/opt/hadoop/share/hadoop/common/lib/woodstox-core-5.0.3.jar:/opt/hadoop/share/hadoop/common/lib/zookeeper-3.4.13.jar:/opt/hadoop/share/hadoop/common/hadoop-common-3.1.3.jar:/opt/hadoop/share/hadoop/common/hadoop-common-3.1.3-tests.jar:/opt/hadoop/share/hadoop/common/hadoop-kms-3.1.3.jar:/opt/hadoop/share/hadoop/common/hadoop-nfs-3.1.3.jar:/opt/hadoop/share/hadoop/common/jdiff:/opt/hadoop/share/hadoop/common/lib:/opt/hadoop/share/hadoop/common/sources:/opt/hadoop/share/hadoop/common/webapps:/opt/hadoop/share/hadoop/hdfs/lib/accessors-smart-1.2.jar:/opt/hadoop/share/hadoop/hdfs/lib/animal-sniffer-annotations-1.17.jar:/opt/hadoop/share/hadoop/hdfs/lib/asm-5.0.4.jar:/opt/hadoop/share/hadoop/hdfs/lib/audience-annotations-0.5.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/avro-1.7.7.jar:/opt/hadoop/share/hadoop/hdfs/lib/checker-qual-2.5.2.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-beanutils-1.9.3.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-cli-1.2.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-codec-1.11.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-collections-3.2.2.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-compress-1.18.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-configuration2-2.1.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-daemon-1.0.13.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-io-2.5.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-lang-2.6.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-lang3-3.4.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-logging-1.1.3.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-math3-3.1.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/commons-net-3.6.jar:/opt/hadoop/share/hadoop/hdfs/lib/curator-client-2.13.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/curator-framework-2.13.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/curator-recipes-2.13.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/error_prone_annotations-2.2.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/failureaccess-1.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/gson-2.2.4.jar:/opt/hadoop/share/hadoop/hdfs/lib/guava-27.0-jre.jar:/opt/hadoop/share/hadoop/hdfs/lib/hadoop-annotations-3.1.3.jar:/opt/hadoop/share/hadoop/hdfs/lib/hadoop-auth-3.1.3.jar:/opt/hadoop/share/hadoop/hdfs/lib/htrace-core4-4.1.0-incubating.jar:/opt/hadoop/share/hadoop/hdfs/lib/httpclient-4.5.2.jar:/opt/hadoop/share/hadoop/hdfs/lib/httpcore-4.4.4.jar:/opt/hadoop/share/hadoop/hdfs/lib/j2objc-annotations-1.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/jackson-annotations-2.7.8.jar:/opt/hadoop/share/hadoop/hdfs/lib/jackson-core-2.7.8.jar:/opt/hadoop/share/hadoop/hdfs/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/hdfs/lib/jackson-databind-2.7.8.jar:/opt/hadoop/share/hadoop/hdfs/lib/jackson-jaxrs-1.9.13.jar:/opt/hadoop/share/hadoop/hdfs/lib/jackson-mapper-asl-1.9.13.jar:/opt/hadoop/share/hadoop/hdfs/lib/jackson-xc-1.9.13.jar:/opt/hadoop/share/hadoop/hdfs/lib/javax.servlet-api-3.1.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/jaxb-api-2.2.11.jar:/opt/hadoop/share/hadoop/hdfs/lib/jaxb-impl-2.2.3-1.jar:/opt/hadoop/share/hadoop/hdfs/lib/jcip-annotations-1.0-1.jar:/opt/hadoop/share/hadoop/hdfs/lib/jersey-core-1.19.jar:/opt/hadoop/share/hadoop/hdfs/lib/jersey-json-1.19.jar:/opt/hadoop/share/hadoop/hdfs/lib/jersey-server-1.19.jar:/opt/hadoop/share/hadoop/hdfs/lib/jersey-servlet-1.19.jar:/opt/hadoop/share/hadoop/hdfs/lib/jettison-1.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-http-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-io-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-security-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-server-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-servlet-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-util-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-util-ajax-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-webapp-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/hdfs/lib/jetty-xml-9.3.24.v20180605.jar:/opt/hadoop/share/hadoop/hdfs/lib/jsch-0.1.54.jar:/opt/hadoop/share/hadoop/hdfs/lib/json-simple-1.1.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/json-smart-2.3.jar:/opt/hadoop/share/hadoop/hdfs/lib/jsr305-3.0.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/jsr311-api-1.1.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerb-admin-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerb-client-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerb-common-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerb-core-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerb-crypto-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerb-identity-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerb-server-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerb-simplekdc-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerb-util-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerby-asn1-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerby-config-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerby-pkix-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerby-util-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/kerby-xdr-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/leveldbjni-all-1.8.jar:/opt/hadoop/share/hadoop/hdfs/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/opt/hadoop/share/hadoop/hdfs/lib/log4j-1.2.17.jar:/opt/hadoop/share/hadoop/hdfs/lib/netty-3.10.5.Final.jar:/opt/hadoop/share/hadoop/hdfs/lib/netty-all-4.0.52.Final.jar:/opt/hadoop/share/hadoop/hdfs/lib/nimbus-jose-jwt-4.41.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/okhttp-2.7.5.jar:/opt/hadoop/share/hadoop/hdfs/lib/okio-1.6.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/paranamer-2.3.jar:/opt/hadoop/share/hadoop/hdfs/lib/protobuf-java-2.5.0.jar:/opt/hadoop/share/hadoop/hdfs/lib/re2j-1.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/snappy-java-1.0.5.jar:/opt/hadoop/share/hadoop/hdfs/lib/stax2-api-3.1.4.jar:/opt/hadoop/share/hadoop/hdfs/lib/token-provider-1.0.1.jar:/opt/hadoop/share/hadoop/hdfs/lib/woodstox-core-5.0.3.jar:/opt/hadoop/share/hadoop/hdfs/lib/zookeeper-3.4.13.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-3.1.3.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-3.1.3-tests.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-client-3.1.3.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-client-3.1.3-tests.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-httpfs-3.1.3.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-native-client-3.1.3.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-native-client-3.1.3-tests.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-nfs-3.1.3.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-rbf-3.1.3.jar:/opt/hadoop/share/hadoop/hdfs/hadoop-hdfs-rbf-3.1.3-tests.jar:/opt/hadoop/share/hadoop/hdfs/jdiff:/opt/hadoop/share/hadoop/hdfs/lib:/opt/hadoop/share/hadoop/hdfs/sources:/opt/hadoop/share/hadoop/hdfs/webapps:/opt/hadoop/share/hadoop/mapreduce/lib/hamcrest-core-1.3.jar:/opt/hadoop/share/hadoop/mapreduce/lib/junit-4.11.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-app-3.1.3.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.1.3.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.1.3.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-3.1.3.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-3.1.3.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-nativetask-3.1.3.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-3.1.3.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-uploader-3.1.3.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.3.jar:/opt/hadoop/share/hadoop/mapreduce/jdiff:/opt/hadoop/share/hadoop/mapreduce/lib:/opt/hadoop/share/hadoop/mapreduce/lib-examples:/opt/hadoop/share/hadoop/mapreduce/sources:/opt/hadoop/share/hadoop/yarn/lib/aopalliance-1.0.jar:/opt/hadoop/share/hadoop/yarn/lib/dnsjava-2.1.7.jar:/opt/hadoop/share/hadoop/yarn/lib/ehcache-3.3.1.jar:/opt/hadoop/share/hadoop/yarn/lib/fst-2.50.jar:/opt/hadoop/share/hadoop/yarn/lib/geronimo-jcache_1.0_spec-1.0-alpha-1.jar:/opt/hadoop/share/hadoop/yarn/lib/guice-4.0.jar:/opt/hadoop/share/hadoop/yarn/lib/guice-servlet-4.0.jar:/opt/hadoop/share/hadoop/yarn/lib/hadoop-yarn-server-timelineservice-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/lib/HikariCP-java7-2.4.12.jar:/opt/hadoop/share/hadoop/yarn/lib/jackson-jaxrs-base-2.7.8.jar:/opt/hadoop/share/hadoop/yarn/lib/jackson-jaxrs-json-provider-2.7.8.jar:/opt/hadoop/share/hadoop/yarn/lib/jackson-module-jaxb-annotations-2.7.8.jar:/opt/hadoop/share/hadoop/yarn/lib/java-util-1.9.0.jar:/opt/hadoop/share/hadoop/yarn/lib/javax.inject-1.jar:/opt/hadoop/share/hadoop/yarn/lib/jersey-client-1.19.jar:/opt/hadoop/share/hadoop/yarn/lib/jersey-guice-1.19.jar:/opt/hadoop/share/hadoop/yarn/lib/json-io-2.5.1.jar:/opt/hadoop/share/hadoop/yarn/lib/metrics-core-3.2.4.jar:/opt/hadoop/share/hadoop/yarn/lib/mssql-jdbc-6.2.1.jre7.jar:/opt/hadoop/share/hadoop/yarn/lib/objenesis-1.0.jar:/opt/hadoop/share/hadoop/yarn/lib/snakeyaml-1.16.jar:/opt/hadoop/share/hadoop/yarn/lib/swagger-annotations-1.5.4.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-api-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-client-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-common-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-registry-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-common-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-nodemanager-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-router-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-tests-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-timeline-pluginstorage-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-server-web-proxy-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-services-api-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/hadoop-yarn-services-core-3.1.3.jar:/opt/hadoop/share/hadoop/yarn/lib:/opt/hadoop/share/hadoop/yarn/sources:/opt/hadoop/share/hadoop/yarn/test:/opt/hadoop/share/hadoop/yarn/timelineservice:/opt/hadoop/share/hadoop/yarn/yarn-service-examples:/opt/hadoop/etc/hadoop:/opt/hadoop/etc/hadoop org.apache.flink.client.cli.CliFrontend run -m yarn-cluster -p 3 -ynm datamiddle-test-submit -c com.xy.core.wc.WordCount -yjm 20m -ytm 50m /opt/runJar/Flink_Tutorial-1.0-SNAPSHOT.jar";
		String[] testArgs = argsStr.split(" ");
		EnvironmentInformation.logEnvironmentInfo(LOG, "Command Line Client", testArgs);

		// 查找flink本地client目录 FLINK_CONF_DIR
		// 1. find the configuration directory
		final String configurationDirectory = getConfigurationDirectoryFromEnv();
		// 加载 flink-conf.yaml
		// 2. load the global configuration
		final Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);

		// 3. load the custom command lines
		final List<CustomCommandLine> customCommandLines = loadCustomCommandLines(
			configuration, // 加载完成的 flink-conf.yaml 配置类
			configurationDirectory);// FLINK_CONF_DIR 目录

		try {
			final CliFrontend cli = new CliFrontend(
				configuration,
				customCommandLines);

			// 认真组件modules以及上下文context
			SecurityUtils.install(new SecurityConfiguration(cli.configuration));
			int retCode = SecurityUtils.getInstalledContext()
					.runSecured(() -> cli.parseAndRun(testArgs));
			System.exit(retCode);
		}
		catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("Fatal error while running command line interface.", strippedThrowable);
			strippedThrowable.printStackTrace();
			System.exit(31);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Miscellaneous Utilities
	// --------------------------------------------------------------------------------------------

	public static String getConfigurationDirectoryFromEnv() {
		String location = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);

		if (location != null) {
			if (new File(location).exists()) {
				return location;
			}
			else {
				throw new RuntimeException("The configuration directory '" + location + "', specified in the '" +
					ConfigConstants.ENV_FLINK_CONF_DIR + "' environment variable, does not exist.");
			}
		}
		else if (new File(CONFIG_DIRECTORY_FALLBACK_1).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_1;
		}
		else if (new File(CONFIG_DIRECTORY_FALLBACK_2).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_2;
		}
		else {
			throw new RuntimeException("The configuration directory was not specified. " +
					"Please specify the directory containing the configuration file through the '" +
				ConfigConstants.ENV_FLINK_CONF_DIR + "' environment variable.");
		}
		return location;
	}

	/**
	 * Writes the given job manager address to the associated configuration object.
	 *
	 * @param address Address to write to the configuration
	 * @param config The configuration to write to
	 */
	static void setJobManagerAddressInConfig(Configuration config, InetSocketAddress address) {
		config.setString(JobManagerOptions.ADDRESS, address.getHostString());
		config.setInteger(JobManagerOptions.PORT, address.getPort());
		config.setString(RestOptions.ADDRESS, address.getHostString());
		config.setInteger(RestOptions.PORT, address.getPort());
	}

	public static List<CustomCommandLine> loadCustomCommandLines(Configuration configuration, String configurationDirectory) {
		List<CustomCommandLine> customCommandLines = new ArrayList<>();
		customCommandLines.add(new GenericCLI(configuration, configurationDirectory));

		//	Command line interface of the YARN session, with a special initialization here
		//	to prefix all options with y/yarn.
		// yarn命令行接口，调用命令
		final String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
		try {
			// 加载yarn命令 行
			customCommandLines.add(
				loadCustomCommandLine(flinkYarnSessionCLI,
					configuration,
					configurationDirectory,
					"y",
					"yarn"));
		} catch (NoClassDefFoundError | Exception e) {
			final String errorYarnSessionCLI = "org.apache.flink.yarn.cli.FallbackYarnSessionCli";
			try {
				LOG.info("Loading FallbackYarnSessionCli");
				customCommandLines.add(
						loadCustomCommandLine(errorYarnSessionCLI, configuration));
			} catch (Exception exception) {
				LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
			}
		}

		//	Tips: DefaultCLI must be added at last, because getActiveCustomCommandLine(..) will get the
		//	      active CustomCommandLine in order and DefaultCLI isActive always return true.
		customCommandLines.add(new DefaultCLI());

		return customCommandLines;
	}

	// --------------------------------------------------------------------------------------------
	//  Custom command-line
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the custom command-line for the arguments.
	 * @param commandLine The input to the command-line.
	 * @return custom command-line which is active (may only be one at a time)
	 */
	public CustomCommandLine validateAndGetActiveCommandLine(CommandLine commandLine) {
		LOG.debug("Custom commandlines: {}", customCommandLines);
		for (CustomCommandLine cli : customCommandLines) {
			LOG.debug("Checking custom commandline {}, isActive: {}", cli, cli.isActive(commandLine));
			if (cli.isActive(commandLine)) {
				return cli;
			}
		}
		throw new IllegalStateException("No valid command-line found.");
	}

	/**
	 * Loads a class from the classpath that implements the CustomCommandLine interface.
	 * @param className The fully-qualified class name to load.
	 * @param params The constructor parameters
	 * 反射构建yarn命令
	 * 					org.apache.flink.yarn.cli.FlinkYarnSessionCli
	 * 					configuration,
	 * 					configurationDirectory,
	 * 					"y",
	 * 					"yarn"
	 */
	private static CustomCommandLine loadCustomCommandLine(String className, Object... params) throws Exception {

		Class<? extends CustomCommandLine> customCliClass =
			Class.forName(className).asSubclass(CustomCommandLine.class);

		// construct class types from the parameters
		Class<?>[] types = new Class<?>[params.length];
		// 参数赋值
		for (int i = 0; i < params.length; i++) {
			checkNotNull(params[i], "Parameters for custom command-lines may not be null.");
			types[i] = params[i].getClass();
		}

		// 获取构造方法
		Constructor<? extends CustomCommandLine> constructor = customCliClass.getConstructor(types);
		// 返回 FlinkYarnSessionCli
		return constructor.newInstance(params);
	}


}
