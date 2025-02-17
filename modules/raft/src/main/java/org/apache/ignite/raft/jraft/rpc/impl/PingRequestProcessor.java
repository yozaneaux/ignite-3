/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.rpc.impl;

import java.util.concurrent.Executor;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.PingRequest;

/**
 * Ping request processor.
 */
public class PingRequestProcessor implements RpcProcessor<PingRequest> {
    private static final IgniteLogger LOG = IgniteLogger.forClass(PingRequestProcessor.class);
    
    /** The executor */
    private final Executor executor;

    /** Message factory. */
    private final RaftMessagesFactory msgFactory;

    /**
     * @param executor The executor.
     */
    public PingRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        this.executor = executor;
        this.msgFactory = msgFactory;
    }

    /** {@inheritDoc} */
    @Override
    public void handleRequest(final RpcContext rpcCtx, final PingRequest request) {
        LOG.debug("Pinged from={}", rpcCtx.getRemoteAddress());
        
        rpcCtx.sendResponse(RaftRpcFactory.DEFAULT.newResponse(msgFactory, 0, "OK"));
    }

    /** {@inheritDoc} */
    @Override
    public String interest() {
        return PingRequest.class.getName();
    }

    /** {@inheritDoc} */
    @Override public Executor executor() {
        return executor;
    }
}
