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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract {@link PipelineExecutor} used to execute {@link Pipeline pipelines} on dedicated
 * (per-job) clusters.
 *
 * @param <ClusterID> the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a
 *     client to the target cluster.
 */
@Internal
public class AbstractJobClusterExecutor<
                ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>>
        implements PipelineExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJobClusterExecutor.class);

    private final ClientFactory clusterClientFactory;

<<<<<<< HEAD
	public AbstractJobClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
		this.clusterClientFactory = checkNotNull(clusterClientFactory);
	}
	// HeryCode:flink per-job 提交方式
	@Override
	public CompletableFuture<JobClient> execute(
		@Nonnull final Pipeline pipeline,
		@Nonnull final Configuration configuration,
		@Nonnull final ClassLoader userCodeClassloader) throws Exception {
		/**
		 * HeryCode  将流图转换为作业图，也就是StreamGraph->JobGraph
		 */
		final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);
		/**
		 * HeryCode 创建集群描述器 返回 YarnClusterDescriptor
		 * 集群描述器包含YarnClient ，然后进行初始化和启动，最后返回集群描述器(包含yarn flink 等配置信息和环境信息)
		 */
		try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(
			configuration)) {

			final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(
				configuration);
			// HeryCode 获取集群指定的特有信息，如 jobmanager、taskManager的内存，slot数量等
			final ClusterSpecification clusterSpecification = clusterClientFactory.getClusterSpecification(
				configuration);
			/**
			 * HeryCode 部署集群 调用YarnClusterDescriptor 的 deployJobCluster 方法
			 */
			final ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor
				.deployJobCluster(clusterSpecification, jobGraph, configAccessor.getDetachedMode());
			LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());

			return CompletableFuture.completedFuture(
				new ClusterClientJobClientAdapter<>(
					clusterClientProvider,
					jobGraph.getJobID(),
					userCodeClassloader));
		}
	}
=======
    public AbstractJobClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
        this.clusterClientFactory = checkNotNull(clusterClientFactory);
    }

    @Override
    public CompletableFuture<JobClient> execute(
            @Nonnull final Pipeline pipeline,
            @Nonnull final Configuration configuration,
            @Nonnull final ClassLoader userCodeClassloader)
            throws Exception {
        final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);

        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                clusterClientFactory.createClusterDescriptor(configuration)) {
            final ExecutionConfigAccessor configAccessor =
                    ExecutionConfigAccessor.fromConfiguration(configuration);

            final ClusterSpecification clusterSpecification =
                    clusterClientFactory.getClusterSpecification(configuration);

            final ClusterClientProvider<ClusterID> clusterClientProvider =
                    clusterDescriptor.deployJobCluster(
                            clusterSpecification, jobGraph, configAccessor.getDetachedMode());
            LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());

            return CompletableFuture.completedFuture(
                    new ClusterClientJobClientAdapter<>(
                            clusterClientProvider, jobGraph.getJobID(), userCodeClassloader));
        }
    }
>>>>>>> release-1.12
}
