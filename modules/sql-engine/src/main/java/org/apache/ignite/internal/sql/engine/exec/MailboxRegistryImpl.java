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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Mailbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.util.NodeLeaveHandler;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

/**
 * MailboxRegistryImpl.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class MailboxRegistryImpl implements MailboxRegistry {
    private static final Predicate<Mailbox<?>> ALWAYS_TRUE = o -> true;

    private final TopologyService topSrvc;

    private final Map<MailboxKey, Outbox<?>> locals;

    private final Map<MailboxKey, Inbox<?>> remotes;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public MailboxRegistryImpl(TopologyService topSrvc) {
        this.topSrvc = topSrvc;

        locals = new ConcurrentHashMap<>();
        remotes = new ConcurrentHashMap<>();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        topSrvc.addEventHandler(new NodeLeaveHandler(this::onNodeLeft));
    }

    /** {@inheritDoc} */
    @Override
    public <T> Inbox<T> register(Inbox<T> inbox) {
        Inbox<T> old = (Inbox<T>) remotes.putIfAbsent(new MailboxKey(inbox.queryId(), inbox.exchangeId()), inbox);

        return old != null ? old : inbox;
    }

    /** {@inheritDoc} */
    @Override
    public void register(Outbox<?> outbox) {
        Outbox<?> res = locals.put(new MailboxKey(outbox.queryId(), outbox.exchangeId()), outbox);

        assert res == null : res;
    }

    /** {@inheritDoc} */
    @Override
    public void unregister(Inbox<?> inbox) {
        remotes.remove(new MailboxKey(inbox.queryId(), inbox.exchangeId()), inbox);
    }

    /** {@inheritDoc} */
    @Override
    public void unregister(Outbox<?> outbox) {
        locals.remove(new MailboxKey(outbox.queryId(), outbox.exchangeId()), outbox);
    }

    /** {@inheritDoc} */
    @Override
    public Outbox<?> outbox(UUID qryId, long exchangeId) {
        return locals.get(new MailboxKey(qryId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override
    public Inbox<?> inbox(UUID qryId, long exchangeId) {
        return remotes.get(new MailboxKey(qryId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Inbox<?>> inboxes(@Nullable UUID qryId, long fragmentId, long exchangeId) {
        return remotes.values().stream()
                .filter(makeFilter(qryId, fragmentId, exchangeId))
                .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Outbox<?>> outboxes(@Nullable UUID qryId, long fragmentId, long exchangeId) {
        return locals.values().stream()
                .filter(makeFilter(qryId, fragmentId, exchangeId))
                .collect(Collectors.toList());
    }

    private void onNodeLeft(ClusterNode node) {
        locals.values().forEach(n -> n.onNodeLeft(node.id()));
        remotes.values().forEach(n -> n.onNodeLeft(node.id()));
    }

    private static Predicate<Mailbox<?>> makeFilter(@Nullable UUID qryId, long fragmentId, long exchangeId) {
        Predicate<Mailbox<?>> filter = ALWAYS_TRUE;
        if (qryId != null) {
            filter = filter.and(mailbox -> Objects.equals(mailbox.queryId(), qryId));
        }
        if (fragmentId != -1) {
            filter = filter.and(mailbox -> mailbox.fragmentId() == fragmentId);
        }
        if (exchangeId != -1) {
            filter = filter.and(mailbox -> mailbox.exchangeId() == exchangeId);
        }

        return filter;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(MailboxRegistryImpl.class, this);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        locals.clear();
        remotes.clear();
    }

    private static class MailboxKey {
        private final UUID qryId;

        private final long exchangeId;

        private MailboxKey(UUID qryId, long exchangeId) {
            this.qryId = qryId;
            this.exchangeId = exchangeId;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MailboxKey that = (MailboxKey) o;

            if (exchangeId != that.exchangeId) {
                return false;
            }
            return qryId.equals(that.qryId);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            int res = qryId.hashCode();
            res = 31 * res + (int) (exchangeId ^ (exchangeId >>> 32));
            return res;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(MailboxKey.class, this);
        }
    }
}
