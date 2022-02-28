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

package org.apache.ignite.internal.network.netty;

import io.netty.bootstrap.Bootstrap;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.TestOnly;

/**
 * Class that manages connections both incoming and outgoing.
 */
public class ConnectionManager {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ConnectionManager.class);

    /** Latest version of the direct marshalling protocol. */
    public static final byte DIRECT_PROTOCOL_VERSION = 1;

    /** Client bootstrap. */
    private final Bootstrap clientBootstrap;

    /** Server. */
    private final NettyServer server;

    /** Serialization service. */
    private final SerializationService serializationService;

    /** Message listeners. */
    private final List<BiConsumer<String, NetworkMessage>> listeners = new CopyOnWriteArrayList<>();

    /** Node consistent id. */
    private final String consistentId;

    /** Client handshake manager factory. */
    private final Supplier<HandshakeManager> clientHandshakeManagerFactory;

    /** Start flag. */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /** Stop flag. */
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    /**
     * Constructor.
     *
     * @param networkConfiguration          Network configuration.
     * @param serializationService          Serialization service.
     * @param consistentId                  Consistent id of this node.
     * @param serverHandshakeManagerFactory Server handshake manager factory.
     * @param clientHandshakeManagerFactory Client handshake manager factory.
     * @param bootstrapFactory              Bootstrap factory.
     */
    public ConnectionManager(
            NetworkView networkConfiguration,
            SerializationService serializationService,
            String consistentId,
            Supplier<HandshakeManager> serverHandshakeManagerFactory,
            Supplier<HandshakeManager> clientHandshakeManagerFactory,
            NettyBootstrapFactory bootstrapFactory
    ) {
        this.serializationService = serializationService;
        this.consistentId = consistentId;
        this.clientHandshakeManagerFactory = clientHandshakeManagerFactory;

        this.server = new NettyServer(
                networkConfiguration,
                serverHandshakeManagerFactory,
                this::onNewIncomingChannel,
                this::onMessage,
                serializationService,
                bootstrapFactory
        );

        this.clientBootstrap = bootstrapFactory.createClientBootstrap();
    }

    /**
     * Starts the server.
     *
     * @throws IgniteInternalException If failed to start.
     */
    public void start() throws IgniteInternalException {
        try {
            boolean wasStarted = started.getAndSet(true);

            if (wasStarted) {
                throw new IgniteInternalException("Attempted to start an already started connection manager");
            }

            if (stopped.get()) {
                throw new IgniteInternalException("Attempted to start an already stopped connection manager");
            }

            server.start().get();

            // LOG.info("Connection created [address=" + server.address() + ']');
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new IgniteInternalException("Failed to start the connection manager: " + cause.getMessage(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException("Interrupted while starting the connection manager", e);
        }
    }

    /**
     * Returns server local address.
     *
     * @return Server local address.
     */
    public SocketAddress getLocalAddress() {
        return server.address();
    }


    private ExecutorService svc = Executors.newFixedThreadPool(20);

    /**
     * Callback that is called upon receiving a new message.
     *
     * @param consistentId Consistent id of the message's sender.
     * @param message New message.
     */
    private void onMessage(String consistentId, NetworkMessage message) {
        // LOG.info("Received " + message.getClass() + " from=" + consistentId);
        svc.submit(() -> {
            listeners.forEach(consumer -> consumer.accept(consistentId, message));
        });
    }

    /**
     * Callback that is called upon new client connected to the server.
     *
     * @param channel Channel from client to this {@link #server}.
     */
    private void onNewIncomingChannel(NettySender channel) {
        // channels.put(channel.consistentId(), channel);
    }

    /**
     * Create new client from this node to specified address.
     *
     * @param key Client key.
     * @return New netty client.
     */
    public CompletableFuture<NettySender> connect(SocketAddress address) {
        var client = new NettyClient(
                address,
                serializationService,
                clientHandshakeManagerFactory.get(),
                this::onMessage
        );

        return client.start(clientBootstrap);
    }

    /**
     * Add incoming message listener.
     *
     * @param listener Message listener.
     */
    public void addListener(BiConsumer<String, NetworkMessage> listener) {
        listeners.add(listener);
    }

    /**
     * Stops the server and all clients.
     */
    public void stop() {
        boolean wasStopped = this.stopped.getAndSet(true);

        if (wasStopped) {
            return;
        }

        try {
            server.stop().join();
        } catch (Exception e) {
            LOG.warn("Failed to stop the ConnectionManager: {}", e.getMessage());
        }

        svc.shutdown();
    }

    /**
     * Returns {@code true} if the connection manager is stopped or is being stopped, {@code false} otherwise.
     *
     * @return {@code true} if the connection manager is stopped or is being stopped, {@code false} otherwise.
     */
    public boolean isStopped() {
        return stopped.get();
    }

    /**
     * Returns connection manager's {@link #server}.
     *
     * @return Connection manager's {@link #server}.
     */
    @TestOnly
    public NettyServer server() {
        return server;
    }

    /**
     * Returns this node's consistent id.
     *
     * @return This node's consistent id.
     */
    @TestOnly
    public String consistentId() {
        return consistentId;
    }
}
