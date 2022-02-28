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
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

public class CommunicationPool {
    private final Deque<Communication> queue = new ConcurrentLinkedDeque<>();
    private final SocketAddress address;
    private final ConnectionManager connectionManager;

    public CommunicationPool(SocketAddress address, ConnectionManager connectionManager) {
        this.address = address;
        this.connectionManager = connectionManager;
    }

    public void release(Communication communication) {
        queue.offer(communication);
    }

    public CompletableFuture<Communication> get() {
        Communication communication = queue.pollFirst();

        if (communication != null) {
            return CompletableFuture.completedFuture(communication);
        }

        return connectionManager.connect(address).thenApply(sender -> {
            return new Communication(this, sender);
        });
    }
}
