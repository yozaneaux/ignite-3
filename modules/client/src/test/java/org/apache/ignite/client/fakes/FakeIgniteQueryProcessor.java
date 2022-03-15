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

package org.apache.ignite.client.fakes;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlCursor;

/**
 * Fake {@link QueryProcessor}.
 */
public class FakeIgniteQueryProcessor implements QueryProcessor {
    @Override
    public List<SqlCursor<List<?>>> query(String schemaName, String qry, Object... params) {
        return Collections.singletonList(new FakeCursor());
    }

    @Override
    public List<SqlCursor<List<?>>> query(QueryContext context, String schemaName, String qry, Object... params) {
        return Collections.singletonList(new FakeCursor());
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() throws Exception {

    }
}
