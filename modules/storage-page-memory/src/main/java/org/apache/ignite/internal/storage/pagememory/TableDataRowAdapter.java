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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.storage.StorageUtils.toByteArray;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.storage.DataRow;

/**
 * Delegating implementation of {@link DataRow}.
 */
class TableDataRowAdapter implements DataRow {
    private final TableDataRow tableDataRow;

    /**
     * Constructor.
     *
     * @param tableDataRow Table data row.
     */
    TableDataRowAdapter(TableDataRow tableDataRow) {
        this.tableDataRow = tableDataRow;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] valueBytes() {
        return toByteArray(value());
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer value() {
        return tableDataRow.value();
    }

    /** {@inheritDoc} */
    @Override
    public byte[] keyBytes() {
        return toByteArray(key());
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer key() {
        return tableDataRow.key();
    }
}
