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

package org.apache.ignite.client.proto.query;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.proto.query.event.BatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.BatchExecuteResult;
import org.apache.ignite.client.proto.query.event.BatchPreparedStmntRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaSchemasRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaSchemasResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryMetadataRequest;
import org.apache.ignite.client.proto.query.event.QueryCloseRequest;
import org.apache.ignite.client.proto.query.event.QueryCloseResult;
import org.apache.ignite.client.proto.query.event.QueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.QueryExecuteResult;
import org.apache.ignite.client.proto.query.event.QueryFetchRequest;
import org.apache.ignite.client.proto.query.event.QueryFetchResult;

/**
 * Jdbc client request handler.
 */
public interface JdbcQueryEventHandler {
    /**
     * {@link QueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Result future.
     */
    CompletableFuture<QueryExecuteResult> queryAsync(QueryExecuteRequest req);

    /**
     * {@link QueryFetchRequest} command handler.
     *
     * @param req Fetch query request.
     * @return Result future.
     */
    CompletableFuture<QueryFetchResult> fetchAsync(QueryFetchRequest req);

    /**
     * {@link BatchExecuteRequest} command handler.
     *
     * @param req Batch query request.
     * @return Result future.
     */
    CompletableFuture<BatchExecuteResult> batchAsync(BatchExecuteRequest req);

    /**
     * {@link BatchPreparedStmntRequest} command handler.
     *
     * @param req Batch query request.
     * @return Result future.
     */
    CompletableFuture<BatchExecuteResult> batchPrepStatementAsync(BatchPreparedStmntRequest req);

    /**
     * {@link QueryCloseRequest} command handler.
     *
     * @param req Close query request.
     * @return Result future.
     */
    CompletableFuture<QueryCloseResult> closeAsync(QueryCloseRequest req);

    /**
     * {@link JdbcMetaTablesRequest} command handler.
     *
     * @param req Jdbc tables metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaTablesResult> tablesMetaAsync(JdbcMetaTablesRequest req);

    /**
     * {@link JdbcMetaColumnsRequest} command handler.
     *
     * @param req Jdbc columns metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaColumnsResult> columnsMetaAsync(JdbcMetaColumnsRequest req);

    /**
     * {@link JdbcMetaSchemasRequest} command handler.
     *
     * @param req Jdbc schemas metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaSchemasResult> schemasMetaAsync(JdbcMetaSchemasRequest req);

    /**
     * {@link JdbcMetaPrimaryKeysRequest} command handler.
     *
     * @param req Jdbc primary keys metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaPrimaryKeysResult> primaryKeysMetaAsync(JdbcMetaPrimaryKeysRequest req);

    /**
     * {@link JdbcQueryMetadataRequest} command handler.
     *
     * @param req Jdbc query metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaColumnsResult> queryMetadataAsync(JdbcQueryMetadataRequest req);
}
