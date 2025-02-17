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
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.configuration.schemas.network.OutboundView;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.jetbrains.annotations.Nullable;
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

    /** Channels map from consistentId to {@link NettySender}. */
    private final Map<String, NettySender> channels = new ConcurrentHashMap<>();

    /** Clients. */
    private final Map<SocketAddress, NettyClient> clients = new ConcurrentHashMap<>();

    /** Serialization service. */
    private final SerializationService serializationService;

    /** Message listeners. */
    private final List<Consumer<InNetworkObject>> listeners = new CopyOnWriteArrayList<>();

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

            LOG.info("Connection created [address=" + server.address() + ']');
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

    /**
     * Gets a {@link NettySender}, that sends data from this node to another node with the specified address.
     *
     * @param consistentId Another node's consistent id.
     * @param address      Another node's address.
     * @return Sender.
     */
    public CompletableFuture<NettySender> channel(@Nullable String consistentId, SocketAddress address) {
        if (consistentId != null) {
            // If consistent id is known, try looking up a channel by consistent id. There can be an outbound connection
            // or an inbound connection associated with that consistent id.
            NettySender channel = channels.compute(
                    consistentId,
                    (addr, sender) -> (sender == null || !sender.isOpen()) ? null : sender
            );

            if (channel != null) {
                return CompletableFuture.completedFuture(channel);
            }
        }

        // Get an existing client or create a new one. NettyClient provides a CompletableFuture that resolves
        // when the client is ready for write operations, so previously started client, that didn't establish connection
        // or didn't perform the handhsake operaton, can be reused.
        NettyClient client = clients.compute(address, (addr, existingClient) ->
                existingClient != null && !existingClient.failedToConnect() && !existingClient.isDisconnected()
                        ? existingClient : connect(addr)
        );

        CompletableFuture<NettySender> sender = client.sender();

        assert sender != null;

        return sender;
    }

    /**
     * Callback that is called upon receiving a new message.
     *
     * @param message New message.
     */
    private void onMessage(InNetworkObject message) {
        listeners.forEach(consumer -> consumer.accept(message));
    }

    /**
     * Callback that is called upon new client connected to the server.
     *
     * @param channel Channel from client to this {@link #server}.
     */
    private void onNewIncomingChannel(NettySender channel) {
        NettySender oldChannel = channels.put(channel.consistentId(), channel);

        if (oldChannel != null) {
            oldChannel.close();
        }
    }

    /**
     * Create new client from this node to specified address.
     *
     * @param address Target address.
     * @return New netty client.
     */
    private NettyClient connect(SocketAddress address) {
        var client = new NettyClient(
                address,
                serializationService,
                clientHandshakeManagerFactory.get(),
                this::onMessage
        );

        client.start(clientBootstrap).whenComplete((sender, throwable) -> {
            if (throwable == null) {
                channels.put(sender.consistentId(), sender);
            } else {
                clients.remove(address);
            }
        });

        return client;
    }

    /**
     * Add incoming message listener.
     *
     * @param listener Message listener.
     */
    public void addListener(Consumer<InNetworkObject> listener) {
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

        Stream<CompletableFuture<Void>> stream = Stream.concat(Stream.concat(
                    clients.values().stream().map(NettyClient::stop),
                    Stream.of(server.stop())
                ),
                channels.values().stream().map(NettySender::closeAsync)
        );

        CompletableFuture<Void> stopFut = CompletableFuture.allOf(stream.toArray(CompletableFuture<?>[]::new));

        try {
            stopFut.join();
        } catch (Exception e) {
            LOG.warn("Failed to stop the ConnectionManager: {}", e.getMessage());
        }
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

    /**
     * Returns collection of all the clients started by this connection manager.
     *
     * @return Collection of all the clients started by this connection manager.
     */
    @TestOnly
    public Collection<NettyClient> clients() {
        return Collections.unmodifiableCollection(clients.values());
    }


    /**
     * Returns map of the channels.
     *
     * @return Map of the channels.
     */
    @TestOnly
    public Map<String, NettySender> channels() {
        return Collections.unmodifiableMap(channels);
    }

    /**
     * Creates a {@link Bootstrap} for clients with channel options provided by a {@link OutboundView}.
     *
     * @param eventLoopGroup      Event loop group for channel handling.
     * @param clientConfiguration Client configuration.
     * @return Bootstrap for clients.
     */
    public static Bootstrap createClientBootstrap(
            EventLoopGroup eventLoopGroup,
            OutboundView clientConfiguration
    ) {
        Bootstrap clientBootstrap = new Bootstrap();

        clientBootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                // See NettyServer#start for netty configuration details.
                .option(ChannelOption.SO_KEEPALIVE, clientConfiguration.soKeepAlive())
                .option(ChannelOption.SO_LINGER, clientConfiguration.soLinger())
                .option(ChannelOption.TCP_NODELAY, clientConfiguration.tcpNoDelay());

        return clientBootstrap;
    }
}
