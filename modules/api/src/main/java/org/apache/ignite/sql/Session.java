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

package org.apache.ignite.sql;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.async.AsyncSession;
import org.apache.ignite.sql.reactive.ReactiveSession;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * SQL Session provides methods for query execution.
 *
 * <p>Session is a stateful object and holds setting that intended to be used as defaults for the new queries. Modifying the session state
 * may affect the queries that are already started within this session. Thus, modifying session state from multiple threads may lead to
 * unexpected behaviour.
 *
 * <p>Session "execute*" methods are thread-safe and can be called from different threads.
 *
 * <p>Closing a session will not affect already running queries.
 */
public interface Session extends AsyncSession, ReactiveSession, AutoCloseable {
    /** Default schema name. */
    String DEFAULT_SCHEMA = "PUBLIC";

    /** Default maximal number of rows in a single page. */
    int DEFAULT_PAGE_SIZE = 1024;

    /**
     * Executes single SQL query.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return SQL query results set.
     * @throws SqlException If failed.
     */
    ResultSet execute(@Nullable Transaction transaction, String query, @Nullable Object... arguments);

    /**
     * Executes single SQL statement.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param arguments Arguments for the statement.
     * @return SQL query results set.
     */
    ResultSet execute(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments);

    /**
     * Executes batched SQL query.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param batch Batch of query arguments.
     * @return Number of rows affected by each query in the batch.
     */
    int[] executeBatch(@Nullable Transaction transaction, String query, BatchedArguments batch);

    /**
     * Executes batched SQL query.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param batch Batch of query arguments.
     * @return Number of rows affected by each query in the batch.
     */
    int[] executeBatch(
            @Nullable Transaction transaction,
            Statement statement,
            BatchedArguments batch
    );

    /**
     * Executes multi-statement SQL query.
     *
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @throws SqlException If failed.
     */
    void executeScript(String query, @Nullable Object... arguments);

    /**
     * Sets default query timeout.
     *
     * @param timeout Query timeout value.
     * @param timeUnit Timeunit.
     */
    void defaultTimeout(long timeout, TimeUnit timeUnit);

    /**
     * Return default query timeout.
     *
     * @param timeUnit Timeunit to convert timeout to.
     * @return Default query timeout in the given timeunit.
     */
    long defaultTimeout(TimeUnit timeUnit);

    /**
     * Sets default schema for the session, which the queries will be executed with.
     *
     * <p>Default schema is used to resolve schema objects by their simple names, those for which schema is not specified in the query
     * text, to their canonical names.
     *
     * @param schema Default schema.
     */
    void defaultSchema(String schema);

    /**
     * Returns session default schema.
     *
     * <p>Default value is {@link #DEFAULT_SCHEMA}.
     *
     * @return Session default schema.
     * @see #defaultSchema(String)
     */
    String defaultSchema();

    /**
     * Sets default page size, which is a maximal amount of results rows that can be fetched once at a time.
     *
     * @param pageSize Maximal amount of rows in a page.
     * @return {@code this} for chaining.
     */
    Session defaultPageSize(int pageSize);

    /**
     * Returns default page size, which is a maximal amount of results rows that can be fetched once at a time.
     *
     * <p>Default value is {@link #DEFAULT_PAGE_SIZE}.
     *
     * @return Maximal amount of rows in a page.
     */
    int defaultPageSize();

    /**
     * Sets session property.
     *
     * @param name Property name.
     * @param value Property value.
     * @return {@code this} for chaining.
     */
    Session property(String name, @Nullable Object value);

    /**
     * Returns session property.
     *
     * @param name Property name.
     * @return Property value.
     */
    @Nullable Object property(String name);

    /**
     * Closes session, cleanup remote session resources, and stops all queries that are running within the current session.
     */
    @Override
    void close();

}
