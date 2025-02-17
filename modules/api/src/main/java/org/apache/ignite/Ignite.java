/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.ApiStatus.Experimental;

/**
 * Ignite API entry point.
 */
public interface Ignite extends AutoCloseable {
    /**
     * Returns ignite node name.
     *
     * @return Ignite node name.
     */
    String name();

    /**
     * Gets an object for manipulate Ignite tables.
     *
     * @return Ignite tables.
     */
    IgniteTables tables();

    /**
     * Returns a transaction facade.
     *
     * @return Ignite transactions.
     */
    IgniteTransactions transactions();

    /**
     * Set new baseline nodes for table assignments.
     *
     * <p>Current implementation has significant restrictions: - Only alive nodes can be a part of new baseline. If any passed nodes are not
     * alive, {@link IgniteException} with appropriate message will be thrown. - Potentially it can be a long operation and current
     * synchronous changePeers-based implementation can't handle this issue well. - No recovery logic supported, if setBaseline fails - it
     * can produce random state of cluster.
     * TODO: IGNITE-14209 issues above must be fixed.
     * TODO: IGNITE-15815 add a test for stopping node and asynchronous implementation.
     *
     * @param baselineNodes Names of baseline nodes.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping,</li>
     *                             <li>{@code baselineNodes} argument is empty or null,</li>
     *                             <li>any node from {@code baselineNodes} is not alive.</li>
     *                         </ul>
     */
    @Experimental
    void setBaseline(Set<String> baselineNodes);

    /**
     * Returns {@link IgniteCompute} which can be used to execute compute jobs.
     *
     * @return compute management object
     * @see IgniteCompute
     * @see ComputeJob
     */
    IgniteCompute compute();

    /**
     * Gets the cluster nodes.
     * NOTE: Temporary API to enable Compute until we have proper Cluster API.
     *
     * @return Collection of cluster nodes.
     */
    Collection<ClusterNode> clusterNodes();

    /**
     * Gets the cluster nodes.
     * NOTE: Temporary API to enable Compute until we have proper Cluster API.
     *
     * @return Collection of cluster nodes.
     */
    CompletableFuture<Collection<ClusterNode>> clusterNodesAsync();
}
