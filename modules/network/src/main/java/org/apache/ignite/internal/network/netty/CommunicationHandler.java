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

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jetbrains.annotations.Nullable;

public class CommunicationHandler {

    private final ConcurrentMap<String, CommunicationPool> connections = new ConcurrentHashMap<>();

    private final ConnectionManager connectionManager;

    public CommunicationHandler(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public CompletableFuture<Communication> communication(@Nullable String consistentId, SocketAddress address) {
        if (consistentId != null) {
            CommunicationPool communicationPool = connections.computeIfAbsent(consistentId, s -> {
                return new CommunicationPool(address, connectionManager);
            });

            return communicationPool.get();
        }

        return connectionManager.connect(address).thenApply((sender) -> afterClientConnected(address, sender));
    }

    public Communication afterClientConnected(SocketAddress address, NettySender sender) {
        CommunicationPool pool = connections.computeIfAbsent(sender.consistentId(), s -> {
            return new CommunicationPool(address, connectionManager);
        });

        return new Communication(pool, sender);
    }

    public boolean isStopped() {
        return connectionManager.isStopped();
    }

    public String consistentId() {
        return connectionManager.consistentId();
    }
}
