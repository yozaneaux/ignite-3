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

package org.apache.ignite.internal.runner.app;

import static org.apache.ignite.internal.recovery.ConfigurationCatchUpListener.CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModule;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalConfigurationStorage;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.recovery.ConfigurationCatchUpListener;
import org.apache.ignite.internal.recovery.RecoveryCompletionFutureFactory;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableTxManagerImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.message.TxMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.jraft.RaftMessagesSerializationRegistryInitializer;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;

/**
 * These tests check node restart scenarios.
 */
@WithSystemProperty(key = CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY, value = "0")
public class ItIgniteNodeRestartTest extends IgniteAbstractTest {
    /** Default node port. */
    private static final int DEFAULT_NODE_PORT = 3344;

    /** Value producer for table data, is used to create data and check it later. */
    private static final IntFunction<String> VALUE_PRODUCER = i -> "val " + i;

    /** Prefix for full table name. */
    private static final String SCHEMA_PREFIX = "PUBLIC.";

    /** Test table name. */
    private static final String TABLE_NAME = "Table1";

    /** Test table name. */
    private static final String TABLE_NAME_2 = "Table2";

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"node\": {\n"
            + "    \"metastorageNodes\":[ {} ]\n"
            + "  },\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Cluster nodes. */
    private static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    /** Cluster nodes. */
    private List<IgniteComponent> partialNode = null;

    /**
     * Stops all started nodes.
     */
    @AfterEach
    public void afterEach() {
        for (int i = 0; i < CLUSTER_NODES.size(); i++) {
            stopNode(i);
        }

        CLUSTER_NODES.clear();

        if (partialNode != null) {
            stopPartialNode(partialNode);

            partialNode = null;
        }
    }

    /**
     * Start some of Ignite components that are able to serve as Ignite node for test purposes.
     *
     * @param name Node name.
     * @param cfgString Configuration string.
     * @return List of started components.
     */
    private List<IgniteComponent> startPartialNode(String name, String cfgString) {
        return startPartialNode(name, cfgString, null);
    }

    /**
     * Start some of Ignite components that are able to serve as Ignite node for test purposes.
     *
     * @param name Node name.
     * @param cfgString Configuration string.
     * @param revisionCallback Callback on storage revision update.
     * @return List of started components.
     */
    private List<IgniteComponent> startPartialNode(String name, String cfgString, Consumer<Long> revisionCallback) {
        Path dir = workDir.resolve(name);

        List<IgniteComponent> res = new ArrayList<>();

        VaultManager vault = createVault(dir);

        ConfigurationModules modules = loadConfigurationModules(log, Thread.currentThread().getContextClassLoader());

        var nodeCfgMgr = new ConfigurationManager(
                modules.local().rootKeys(),
                modules.local().validators(),
                new LocalConfigurationStorage(vault),
                modules.local().internalSchemaExtensions(),
                modules.local().polymorphicSchemaExtensions()
        );

        NetworkConfiguration networkConfiguration = nodeCfgMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY);

        MessageSerializationRegistryImpl serializationRegistry = new MessageSerializationRegistryImpl();
        RaftMessagesSerializationRegistryInitializer.registerFactories(serializationRegistry);
        TxMessagesSerializationRegistryInitializer.registerFactories(serializationRegistry);

        var clusterLocalConfiguration = new ClusterLocalConfiguration(name, serializationRegistry);

        var nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration, clusterLocalConfiguration.getName());

        var clusterSvc = new ScaleCubeClusterServiceFactory().createClusterService(
                clusterLocalConfiguration,
                networkConfiguration,
                nettyBootstrapFactory
        );

        var raftMgr = new Loza(clusterSvc, dir);

        var txManager = new TableTxManagerImpl(clusterSvc, new HeapLockManager());

        var metaStorageMgr = new MetaStorageManager(
                vault,
                nodeCfgMgr,
                clusterSvc,
                raftMgr,
                new RocksDbKeyValueStorage(dir.resolve(Paths.get("metastorage")))
        );

        var cfgStorage = new DistributedConfigurationStorage(metaStorageMgr, vault);

        var clusterCfgMgr = new ConfigurationManager(
                modules.distributed().rootKeys(),
                modules.distributed().validators(),
                cfgStorage,
                modules.distributed().internalSchemaExtensions(),
                modules.distributed().polymorphicSchemaExtensions()
        );

        Consumer<Consumer<Long>> registry = (c) -> {
            clusterCfgMgr.configurationRegistry().listenUpdateStorageRevision(newStorageRevision -> {
                c.accept(newStorageRevision);

                return CompletableFuture.completedFuture(null);
            });
        };

        DataStorageManager dataStorageManager = new DataStorageManager(
                clusterCfgMgr.configurationRegistry(),
                getPartitionsStorePath(dir)
        );

        TableManager tableManager = new TableManager(
                registry,
                clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY),
                raftMgr,
                Mockito.mock(BaselineManager.class),
                clusterSvc.topologyService(),
                txManager,
                dataStorageManager
        );

        // Preparing the result map.

        res.add(vault);
        res.add(nodeCfgMgr);

        // Start.

        vault.start();
        vault.putName(name).join();

        nodeCfgMgr.start();

        // Node configuration manager bootstrap.
        if (cfgString != null) {
            try {
                nodeCfgMgr.bootstrap(cfgString);
            } catch (Exception e) {
                throw new IgniteException("Unable to parse user-specific configuration.", e);
            }
        } else {
            nodeCfgMgr.configurationRegistry().initializeDefaults();
        }

        // Start the remaining components.
        List<IgniteComponent> otherComponents = List.of(
                nettyBootstrapFactory,
                clusterSvc,
                raftMgr,
                txManager,
                metaStorageMgr,
                clusterCfgMgr,
                dataStorageManager,
                tableManager
        );

        for (IgniteComponent component : otherComponents) {
            component.start();

            res.add(component);
        }

        AtomicLong lastRevision = new AtomicLong();

        Consumer<Long> revisionCallback0 = rev -> {
            if (revisionCallback != null) {
                revisionCallback.accept(rev);
            }

            lastRevision.set(rev);
        };

        CompletableFuture<Void> configurationCatchUpFuture = RecoveryCompletionFutureFactory.create(
                metaStorageMgr,
                clusterCfgMgr,
                fut -> new TestConfigurationCatchUpListener(cfgStorage, fut, revisionCallback0)
        );

        nodeCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners();
        clusterCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners();

        // Deploy all registered watches because all components are ready and have registered their listeners.
        try {
            metaStorageMgr.deployWatches();
        } catch (NodeStoppingException e) {
            e.printStackTrace();
        }

        configurationCatchUpFuture.join();

        log.info("Completed recovery on partially started node, last revision applied: " + lastRevision.get()
                + ", acceptableDifference: " + IgniteSystemProperties.getInteger(CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY, 100)
        );

        partialNode = res;

        return res;
    }

    /**
     * Stop partially started Ignite node that is represented by a list of components.
     *
     * @param componentsList A list of components.
     */
    private void stopPartialNode(List<IgniteComponent> componentsList) {
        ListIterator<IgniteComponent> iter = componentsList.listIterator(componentsList.size());

        while (iter.hasPrevious()) {
            IgniteComponent prev = iter.previous();

            prev.beforeNodeStop();
        }

        iter = componentsList.listIterator(componentsList.size());

        while (iter.hasPrevious()) {
            IgniteComponent prev = iter.previous();

            try {
                prev.stop();
            } catch (Exception e) {
                log.error("Error during component stop", e);
            }
        }
    }

    /**
     * Starts the Vault component.
     */
    public static VaultManager createVault(Path workDir) {
        Path vaultPath = workDir.resolve(Paths.get("vault"));

        try {
            Files.createDirectories(vaultPath);
        } catch (IOException e) {
            throw new IgniteInternalException(e);
        }

        return new VaultManager(new PersistentVaultService(vaultPath));
    }

    /**
     * Returns a path to the partitions store directory. Creates a directory if it doesn't exist.
     *
     * @param workDir Ignite work directory.
     * @return Partitions store path.
     */
    @NotNull
    public static Path getPartitionsStorePath(Path workDir) {
        Path partitionsStore = workDir.resolve(Paths.get("db"));

        try {
            Files.createDirectories(partitionsStore);
        } catch (IOException e) {
            throw new IgniteInternalException("Failed to create directory for partitions storage: " + e.getMessage(), e);
        }

        return partitionsStore;
    }

    /**
     * Load configuration modules.
     *
     * @param log Log.
     * @param classLoader Class loader.
     * @return Configuration modules.
     */
    public static ConfigurationModules loadConfigurationModules(IgniteLogger log, ClassLoader classLoader) {
        var modulesProvider = new ServiceLoaderModulesProvider();
        List<ConfigurationModule> modules = modulesProvider.modules(classLoader);

        if (log.isInfoEnabled()) {
            log.info("Configuration modules loaded: {}", modules);
        }

        if (modules.isEmpty()) {
            throw new IllegalStateException("No configuration modules were loaded, this means Ignite cannot start. "
                + "Please make sure that the classloader for loading services is correct.");
        }

        var configModules = new ConfigurationModules(modules);

        if (log.isInfoEnabled()) {
            log.info("Local root keys: {}", configModules.local().rootKeys());
            log.info("Distributed root keys: {}", configModules.distributed().rootKeys());
        }

        return configModules;
    }

    /**
     * Start node with the given parameters.
     *
     * @param idx Node index, is used to stop the node later, see {@link #stopNode(int)}.
     * @param nodeName Node name.
     * @param cfgString Configuration string.
     * @param workDir Working directory.
     * @return Created node instance.
     */
    private IgniteImpl startNode(int idx, String nodeName, String cfgString, Path workDir) {
        IgniteImpl ignite = (IgniteImpl) IgnitionManager.start(nodeName, cfgString, workDir);

        assertTrue(CLUSTER_NODES.size() == idx || CLUSTER_NODES.get(idx) == null);

        CLUSTER_NODES.add(idx, ignite);

        return ignite;
    }

    /**
     * Start node with the given parameters.
     *
     * @param testInfo Test info.
     * @param idx Node index, is used to stop the node later, see {@link #stopNode(int)}.
     * @param predefinedNodeName Predefined node name, can be null.
     * @param predefinedPort Predefined port, is {@code null} then default port is used.
     * @param cfg Configuration string, can be auto-generated if {@code null}.
     * @return Created node instance.
     */
    private IgniteImpl startNode(
            TestInfo testInfo,
            int idx,
            @Nullable String predefinedNodeName,
            @Nullable Integer predefinedPort,
            @Nullable String cfg
    ) {
        int port = predefinedPort == null ? DEFAULT_NODE_PORT + idx : predefinedPort;
        String nodeName = predefinedNodeName == null ? testNodeName(testInfo, port) : predefinedNodeName;
        String cfgString = configurationString(testInfo, idx, cfg, predefinedPort);

        return startNode(idx, nodeName, cfgString, workDir.resolve(nodeName));
    }

    /**
     * Start node with the given parameters.
     *
     * @param testInfo Test info.
     * @param idx Node index, is used to stop the node later, see {@link #stopNode(int)}.
     * @return Created node instance.
     */
    private IgniteImpl startNode(TestInfo testInfo, int idx) {
        return startNode(testInfo, idx, null, null, null);
    }

    /**
     * Build a configuration string.
     *
     * @param testInfo Test info.
     * @param idx Node index.
     * @param cfg Optional configuration string.
     * @param predefinedPort  Predefined port.
     * @return Configuration string.
     */
    private String configurationString(TestInfo testInfo, int idx, @Nullable String cfg, @Nullable Integer predefinedPort) {
        int port = predefinedPort == null ? DEFAULT_NODE_PORT + idx : predefinedPort;
        int connectPort = predefinedPort == null ? DEFAULT_NODE_PORT : predefinedPort;
        String connectAddr = "\"localhost:" + connectPort + '\"';
        String metastorageNodeName = testNodeName(testInfo, connectPort);

        return cfg == null
            ? IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, metastorageNodeName, port, connectAddr)
            : cfg;
    }

    /**
     * Stop the node with given index.
     *
     * @param idx Node index.
     */
    private void stopNode(int idx) {
        Ignite node = CLUSTER_NODES.get(idx);

        if (node != null) {
            IgnitionManager.stop(node.name());

            CLUSTER_NODES.set(idx, null);
        }
    }

    /**
     * Restarts empty node.
     */
    @Test
    public void emptyNodeTest(TestInfo testInfo) {
        final int defaultPort = 47500;

        String nodeName = testNodeName(testInfo, defaultPort);

        IgniteImpl ignite = startNode(0, nodeName, null, workDir);

        int nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(defaultPort, nodePort);

        stopNode(0);

        ignite = startNode(0, nodeName, null, workDir);

        nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(defaultPort, nodePort);
    }

    /**
     * Restarts a node with changing configuration.
     */
    @Test
    public void changeConfigurationOnStartTest(TestInfo testInfo) {
        IgniteImpl ignite = startNode(testInfo, 0);

        int nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        stopNode(0);

        int newPort = 3322;

        String updateCfg = "network.port=" + newPort;

        ignite = startNode(testInfo, 0, null, newPort, updateCfg);

        nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(newPort, nodePort);
    }

    /**
     * Checks that the only one non-default property overwrites after another configuration is passed on the node restart.
     */
    @Test
    public void twoCustomPropertiesTest(TestInfo testInfo) {
        String startCfg = "network: {\n"
                + "  port:3344,\n"
                + "  nodeFinder: {netClusterNodes:[ \"localhost:3344\" ]}\n"
                + "}";

        IgniteImpl ignite = startNode(testInfo, 0, null, 3344, startCfg);

        String nodeName = ignite.name();

        assertEquals(
                3344,
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value()
        );

        assertArrayEquals(
                new String[]{"localhost:3344"},
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).nodeFinder().netClusterNodes().value()
        );

        stopNode(0);

        ignite = startNode(testInfo, 0, nodeName, null, "network.nodeFinder.netClusterNodes=[ \"localhost:3344\", \"localhost:3343\" ]");

        assertEquals(
                3344,
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value()
        );

        assertArrayEquals(
                new String[]{"localhost:3344", "localhost:3343"},
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).nodeFinder().netClusterNodes().value()
        );
    }

    /**
     * Restarts the node which stores some data.
     */
    @Test
    public void nodeWithDataTest(TestInfo testInfo) {
        Ignite ignite = startNode(testInfo, 0);

        createTableWithData(ignite, TABLE_NAME, 1);

        stopNode(0);

        ignite = startNode(testInfo, 0);

        checkTableWithData(ignite, TABLE_NAME);
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts.
     * Nodes restart in the same order when they started at first.
     *
     * @param testInfo Test information object.
     */
    @Test
    public void testTwoNodesRestartDirect(TestInfo testInfo) {
        twoNodesRestart(testInfo, true);
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts.
     * Nodes restart in reverse order when they started at first.
     *
     * @param testInfo Test information object.
     */
    @Test
    @Disabled("IGNITE-16034 Unblock a node start that happenes before Metastorage is ready")
    public void testTwoNodesRestartReverse(TestInfo testInfo) {
        twoNodesRestart(testInfo, false);
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts.
     *
     * @param testInfo Test information object.
     * @param directOrder When the parameter is true, nodes restart in direct order, otherwise they restart in reverse order.
     */
    private void twoNodesRestart(TestInfo testInfo, boolean directOrder) {
        Ignite ignite = startNode(testInfo, 0);

        startNode(testInfo, 1);

        createTableWithData(ignite, TABLE_NAME, 2);
        createTableWithData(ignite, TABLE_NAME_2, 2);

        stopNode(0);
        stopNode(1);

        if (directOrder) {
            startNode(testInfo, 0);
            ignite = startNode(testInfo, 1);
        } else {
            ignite = startNode(testInfo, 1);
            startNode(testInfo, 0);
        }

        checkTableWithData(ignite, TABLE_NAME);
        checkTableWithData(ignite, TABLE_NAME_2);
    }

    /**
     * Find component of a given type in list.
     *
     * @param components Components list.
     * @param cls Class.
     * @param <T> Type parameter.
     * @return Ignite component.
     */
    private <T extends IgniteComponent> T findComponent(List<IgniteComponent> components, Class<T> cls) {
        for (IgniteComponent component : components) {
            if (cls.isAssignableFrom(component.getClass())) {
                return (T) component;
            }
        }

        return null;
    }

    /**
     * Checks that one node in a cluster of 2 nodes is able to restart and recover a table that was created when this node was absent.
     * Also checks that the table created before node stop, is not available when majority if lost.
     *
     * @param testInfo Test info.
     */
    @Test
    public void testOneNodeRestartWithGap(TestInfo testInfo) {
        Ignite ignite = startNode(testInfo, 0);

        String cfgString = configurationString(testInfo, 1, null, null);

        List<IgniteComponent> components = startPartialNode(testNodeName(testInfo, DEFAULT_NODE_PORT + 1), cfgString);

        createTableWithData(ignite, TABLE_NAME, 2);

        stopPartialNode(components);

        Table table = ignite.tables().table(SCHEMA_PREFIX + TABLE_NAME);

        assertNotNull(table);

        assertThrowsWithCause(() -> table.keyValueView().get(null, Tuple.create().set("id", 0)), TimeoutException.class);

        createTableWithData(ignite, TABLE_NAME_2, 1, 1);

        components = startPartialNode(testNodeName(testInfo, DEFAULT_NODE_PORT + 1), cfgString);

        TableManager tableManager = findComponent(components, TableManager.class);

        assertNotNull(tableManager);

        assertNotNull(tableManager.latestTables().get(SCHEMA_PREFIX + TABLE_NAME.toUpperCase()));
        assertNotNull(tableManager.latestTables().get(SCHEMA_PREFIX + TABLE_NAME_2.toUpperCase()));
    }

    /**
     * Checks that the table created in cluster of 2 nodes, is recovered on a node after restart of this node.
     *
     * @param testInfo Test info.
     */
    @Test
    public void testRecoveryOnOneNode(TestInfo testInfo) {
        Ignite ignite = startNode(testInfo, 0);

        String cfgString = configurationString(testInfo, 1, null, null);

        List<IgniteComponent> components = startPartialNode(testNodeName(testInfo, DEFAULT_NODE_PORT + 1), cfgString);

        createTableWithData(ignite, TABLE_NAME, 2, 1);

        stopPartialNode(components);

        components = startPartialNode(testNodeName(testInfo, DEFAULT_NODE_PORT + 1), cfgString);

        TableManager tableManager = findComponent(components, TableManager.class);

        assertNotNull(tableManager);

        assertNotNull(tableManager.latestTables().get(SCHEMA_PREFIX + TABLE_NAME.toUpperCase()));
    }

    /**
     * Checks that a cluster is able to restart when some changes were made in configuration.
     *
     * @param testInfo Test info.
     */
    @Test
    public void testRestartDiffConfig(TestInfo testInfo) {
        Ignite ignite0 = startNode(testInfo, 0);
        Ignite ignite1 = startNode(testInfo, 1);

        createTableWithData(ignite0, TABLE_NAME, 2);
        createTableWithData(ignite0, TABLE_NAME_2, 2);

        String igniteName = ignite1.name();

        stopNode(0);
        stopNode(1);

        ignite0 = startNode(testInfo, 0);

        String metastorageName = ignite0.name();

        String cfgString = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG,
                metastorageName,
                DEFAULT_NODE_PORT + 11,
                "\"localhost:" + (DEFAULT_NODE_PORT) + '\"'
        );

        List<IgniteComponent> components = startPartialNode(igniteName, cfgString);

        TableManager tableManager = findComponent(components, TableManager.class);

        assertNotNull(tableManager.latestTables().get(SCHEMA_PREFIX + TABLE_NAME.toUpperCase()));
    }

    /**
     * The test for node restart when there is a gap between the node local configuration and distributed configuration.
     *
     * @param testInfo Test info.
     */
    @Test
    @WithSystemProperty(key = CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY, value = "0")
    public void testCfgGapWithoutData(TestInfo testInfo) {
        final int nodes = 3;

        for (int i = 0; i < nodes; i++) {
            startNode(testInfo, i);
        }

        createTableWithData(CLUSTER_NODES.get(0), TABLE_NAME, nodes);

        String igniteName = CLUSTER_NODES.get(nodes - 1).name();

        log.info("Stopping the node.");

        stopNode(nodes - 1);

        createTableWithData(CLUSTER_NODES.get(0), TABLE_NAME_2, nodes);

        log.info("Starting the node.");

        List<IgniteComponent> components = startPartialNode(igniteName, configurationString(testInfo, nodes - 1, null, null));

        TableManager tableManager = findComponent(components, TableManager.class);

        assertNotNull(tableManager.latestTables().get(SCHEMA_PREFIX + TABLE_NAME.toUpperCase()));
        assertNotNull(tableManager.latestTables().get(SCHEMA_PREFIX + TABLE_NAME_2.toUpperCase()));
    }

    /**
     * The test for node restart when there is a gap between the node local configuration and distributed configuration,
     * and metastorage group stops for some time while restarting node is being recovered. The recovery process should
     * continue and eventually succeed after metastorage group starts again.
     *
     * @param testInfo Test info.
     */
    @Test
    @WithSystemProperty(key = CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY, value = "0")
    public void testMetastorageStop(TestInfo testInfo) {
        final int nodes = 3;
        final int cfgGap = 4;

        for (int i = 0; i < nodes; i++) {
            startNode(testInfo, i);
        }

        String igniteName = CLUSTER_NODES.get(nodes - 1).name();

        log.info("Stopping the node.");

        stopNode(nodes - 1);

        for (int i = 0; i < cfgGap; i++) {
            createTableWithData(CLUSTER_NODES.get(0), "t" + i, nodes, 1);
        }

        log.info("Starting the node.");

        List<IgniteComponent> components = startPartialNode(
                igniteName,
                configurationString(testInfo, nodes - 1, null, null),
                rev -> {
                    log.info("Partially started node: applying revision: " + rev);

                    if (rev == cfgGap / 2) {
                        log.info("Stopping METASTORAGE");

                        stopNode(0);

                        startNode(testInfo, 0);

                        log.info("Restarted METASTORAGE");
                    }
                }
        );

        TableManager tableManager = findComponent(components, TableManager.class);

        for (int i = 0; i < cfgGap; i++) {
            assertNotNull(tableManager.latestTables().get(SCHEMA_PREFIX + "T" + i), SCHEMA_PREFIX + "T" + i);
        }
    }

    /**
     * The test for node restart when there is a gap between the node local configuration and distributed configuration.
     *
     * @param testInfo Test info.
     */
    @Test
    @Disabled("IGNITE-16718")
    public void testCfgGap(TestInfo testInfo) {
        final int nodes = 4;

        for (int i = 0; i < nodes; i++) {
            startNode(testInfo, i);
        }

        createTableWithData(CLUSTER_NODES.get(0), "t1", nodes);

        String igniteName = CLUSTER_NODES.get(nodes - 1).name();

        log.info("Stopping the node.");

        stopNode(nodes - 1);

        checkTableWithData(CLUSTER_NODES.get(0), "t1");

        createTableWithData(CLUSTER_NODES.get(0), "t2", nodes);

        log.info("Starting the node.");

        Ignite newNode = IgnitionManager.start(igniteName, null, workDir.resolve(igniteName));

        CLUSTER_NODES.set(nodes - 1, newNode);

        checkTableWithData(CLUSTER_NODES.get(0), "t1");
        checkTableWithData(CLUSTER_NODES.get(0), "t2");

        checkTableWithData(CLUSTER_NODES.get(nodes - 1), "t1");
        checkTableWithData(CLUSTER_NODES.get(nodes - 1), "t2");
    }

    /**
     * Checks the table exists and validates all data in it.
     *
     * @param ignite Ignite.
     * @param name Table name.
     */
    private void checkTableWithData(Ignite ignite, String name) {
        Table table = ignite.tables().table("PUBLIC." + name);

        assertNotNull(table);

        for (int i = 0; i < 100; i++) {
            Tuple row = table.keyValueView().get(null, Tuple.create().set("id", i));

            assertEquals(VALUE_PRODUCER.apply(i), row.stringValue("name"));
        }
    }

    /**
     * Creates a table and load data to it.
     *
     * @param ignite Ignite.
     * @param name Table name.
     * @param replicas Replica factor.
     */
    private void createTableWithData(Ignite ignite, String name, int replicas) {
        createTableWithData(ignite, name, replicas, 10);
    }

    /**
     * Creates a table and load data to it.
     *
     * @param ignite Ignite.
     * @param name Table name.
     * @param replicas Replica factor.
     * @param partitions Partitions count.
     */
    private void createTableWithData(Ignite ignite, String name, int replicas, int partitions) {
        TableDefinition scmTbl1 = SchemaBuilders.tableBuilder("PUBLIC", name).columns(
                SchemaBuilders.column("id", ColumnType.INT32).build(),
                SchemaBuilders.column("name", ColumnType.string()).asNullable(true).build()
        ).withPrimaryKey(
                SchemaBuilders.primaryKey()
                        .withColumns("id")
                        .build()
        ).build();

        Table table = ignite.tables().createTable(
                scmTbl1.canonicalName(),
                tbl -> convert(scmTbl1, tbl).changePartitions(10).changeReplicas(replicas).changePartitions(partitions)
        );

        for (int i = 0; i < 100; i++) {
            Tuple key = Tuple.create().set("id", i);
            Tuple val = Tuple.create().set("name", VALUE_PRODUCER.apply(i));

            table.keyValueView().put(null, key, val);
        }
    }

    /**
     * Configuration catch-up listener for test.
     */
    private static class TestConfigurationCatchUpListener extends ConfigurationCatchUpListener {
        /** Callback called on revision update. */
        private final Consumer<Long> revisionCallback;

        /**
         * Constructor.
         *
         * @param cfgStorage Configuration storage.
         * @param catchUpFuture Catch-up future.
         */
        public TestConfigurationCatchUpListener(
                ConfigurationStorage cfgStorage,
                CompletableFuture<Void> catchUpFuture,
                Consumer<Long> revisionCallback
        ) {
            super(cfgStorage, catchUpFuture, log);

            this.revisionCallback = revisionCallback;
        }

        /** {@inheritDoc} */
        @Override public CompletableFuture<?> onUpdate(long appliedRevision) {
            if (revisionCallback != null) {
                revisionCallback.accept(appliedRevision);
            }

            return super.onUpdate(appliedRevision);
        }
    }
}
