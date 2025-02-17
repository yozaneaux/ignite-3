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

package org.apache.ignite.raft.server;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Abstract test for raft server.
 */
abstract class RaftServerAbstractTest {
    protected static final IgniteLogger LOG = IgniteLogger.forClass(RaftServerAbstractTest.class);

    protected static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Network factory. */
    protected static final TestScaleCubeClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /**
     * Server port offset.
     */
    protected static final int PORT = 20010;

    /** Test info. */
    TestInfo testInfo;

    private final List<ClusterService> clusterServices = new ArrayList<>();

    @BeforeEach
    void initTestInfo(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    protected void after() throws Exception {
        clusterServices.forEach(ClusterService::stop);
    }

    /**
     * Creates a client cluster view.
     *
     * @param port    Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    protected ClusterService clusterService(int port, List<NetworkAddress> servers, boolean start) {
        var network = ClusterServiceTestUtils.clusterService(
                testInfo,
                port,
                new StaticNodeFinder(servers),
                NETWORK_FACTORY
        );

        if (start) {
            network.start();
        }

        clusterServices.add(network);

        return network;
    }
}
