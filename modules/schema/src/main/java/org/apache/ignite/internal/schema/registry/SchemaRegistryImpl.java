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

package org.apache.ignite.internal.schema.registry;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.schema.row.Row;
import org.jetbrains.annotations.Nullable;

/**
 * Caching registry of actual schema descriptors for a table.
 */
public class SchemaRegistryImpl implements SchemaRegistry {
    /** Initial schema version. */
    public static final int INITIAL_SCHEMA_VERSION = -1;

    /** Cached schemas. */
    private final ConcurrentNavigableMap<Integer, SchemaDescriptor> schemaCache = new ConcurrentSkipListMap<>();

    /** Column mappers cache. */
    private final Map<Long, ColumnMapper> mappingCache = new ConcurrentHashMap<>();

    /** Last registered version. */
    private volatile int lastVer;

    /** Schema store. */
    private final Function<Integer, SchemaDescriptor> history;

    /** The method to provide the latest schema version on cluster. */
    private final IntSupplier latestVersionStore;

    /**
     * Default constructor.
     *
     * @param history            Schema history.
     * @param latestVersionStore The method to provide the latest version of the schema.
     */
    public SchemaRegistryImpl(Function<Integer, SchemaDescriptor> history, IntSupplier latestVersionStore) {
        this(INITIAL_SCHEMA_VERSION, history, latestVersionStore);
    }

    /**
     * Constructor.
     *
     * @param initialVer         Initial version.
     * @param history            Schema history.
     * @param latestVersionStore The method to provide the latest version of the schema.
     */
    public SchemaRegistryImpl(int initialVer, Function<Integer, SchemaDescriptor> history, IntSupplier latestVersionStore) {
        lastVer = initialVer;
        this.history = history;
        this.latestVersionStore = latestVersionStore;
    }

    /** {@inheritDoc} */
    @Override
    public SchemaDescriptor schema(int ver) {
        if (ver == 0) {
            // Use last version (any version may be used) for 0 version, that mean row doens't contain value.
            ver = lastVer;
        }

        SchemaDescriptor desc = schemaCache.get(ver);

        if (desc != null) {
            return desc;
        }

        desc = history.apply(ver);

        if (desc != null) {
            schemaCache.putIfAbsent(ver, desc);

            return desc;
        }

        if (lastVer < ver || ver <= 0) {
            throw new SchemaRegistryException("Incorrect schema version requested: ver=" + ver);
        } else {
            throw new SchemaRegistryException("Failed to find schema: ver=" + ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable SchemaDescriptor schema() {
        final int lastVer0 = lastVer;

        if (lastVer0 == INITIAL_SCHEMA_VERSION) {
            return null;
        }

        return schema(lastVer0);
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor waitLatestSchema() {
        int lastVer0 = latestVersionStore.getAsInt();

        if (lastVer0 == INITIAL_SCHEMA_VERSION) {
            return schema();
        }

        assert lastVer <= lastVer0 : "Cached schema is earlier than consensus [lastVer=" + lastVer
                + ", consLastVer=" + lastVer0 + ']';

        return schema(lastVer0);
    }

    /** {@inheritDoc} */
    @Override
    public int lastSchemaVersion() {
        return lastVer;
    }

    /** {@inheritDoc} */
    @Override
    public Row resolve(BinaryRow row) {
        final SchemaDescriptor curSchema = waitLatestSchema();

        return resolveInternal(row, curSchema);
    }

    /** {@inheritDoc} */
    @Override
    public Row resolve(BinaryRow row, SchemaDescriptor schemaDescriptor) {
        return resolveInternal(row, schemaDescriptor);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Row> resolve(Collection<BinaryRow> rows) {
        final SchemaDescriptor curSchema = waitLatestSchema();

        return rows.stream().map(row -> resolveInternal(row, curSchema)).collect(toList());
    }

    /**
     * Resolves a schema for row.
     * The method is optimal when the latest schema is already gotten.
     *
     * @param row       Binary row.
     * @param curSchema The latest available local schema.
     * @return Schema-aware rows.
     */
    @Nullable
    private Row resolveInternal(BinaryRow row, SchemaDescriptor curSchema) {
        if (row == null) {
            return null;
        }

        final SchemaDescriptor rowSchema = schema(row.schemaVersion());

        if (curSchema.version() == rowSchema.version()) {
            return new Row(rowSchema, row);
        }

        ColumnMapper mapping = resolveMapping(curSchema, rowSchema);

        return new UpgradingRowAdapter(curSchema, rowSchema, row, mapping);
    }

    /**
     * ResolveMapping.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param curSchema Target schema.
     * @param rowSchema Row schema.
     * @return Column mapper for target schema.
     */
    ColumnMapper resolveMapping(SchemaDescriptor curSchema, SchemaDescriptor rowSchema) {
        assert curSchema.version() > rowSchema.version();

        if (curSchema.version() == rowSchema.version() + 1) {
            return curSchema.columnMapping();
        }

        final long mappingKey = (((long) curSchema.version()) << 32) | (rowSchema.version());

        ColumnMapper mapping;

        if ((mapping = mappingCache.get(mappingKey)) != null) {
            return mapping;
        }

        mapping = schema(rowSchema.version() + 1).columnMapping();

        for (int i = rowSchema.version() + 2; i <= curSchema.version(); i++) {
            mapping = ColumnMapping.mergeMapping(mapping, schema(i));
        }

        mappingCache.putIfAbsent(mappingKey, mapping);

        return mapping;
    }

    /**
     * Registers new schema.
     *
     * @param desc Schema descriptor.
     * @throws SchemaRegistrationConflictException If schema of provided version was already registered.
     * @throws SchemaRegistryException             If schema of incorrect version provided.
     */
    public void onSchemaRegistered(SchemaDescriptor desc) {
        if (lastVer == INITIAL_SCHEMA_VERSION) {
            if (desc.version() != 1) {
                throw new SchemaRegistryException(
                        "Try to register schema of wrong version: ver=" + desc.version() + ", lastVer=" + lastVer);
            }
        } else if (desc.version() != lastVer + 1) {
            if (desc.version() > 0 && desc.version() <= lastVer) {
                throw new SchemaRegistrationConflictException("Schema with given version has been already registered: " + desc.version());
            }

            throw new SchemaRegistryException("Try to register schema of wrong version: ver=" + desc.version() + ", lastVer=" + lastVer);
        }

        schemaCache.put(desc.version(), desc);

        lastVer = desc.version();
    }

    /**
     * Cleanup given schema version from history.
     *
     * @param ver Schema version to remove.
     * @throws SchemaRegistryException If incorrect schema version provided.
     */
    public void onSchemaDropped(int ver) {
        if (ver >= lastVer || ver <= 0 || schemaCache.keySet().first() < ver) {
            throw new SchemaRegistryException("Incorrect schema version to clean up to: " + ver);
        }

        if (schemaCache.remove(ver) != null) {
            mappingCache.keySet().removeIf(k -> (k & 0xFFFF_FFFFL) == ver);
        }
    }

    /**
     * For test purposes only.
     *
     * @return ColumnMapping cache.
     */
    Map<Long, ColumnMapper> mappingCache() {
        return mappingCache;
    }
}
