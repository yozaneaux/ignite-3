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

package org.apache.ignite.internal.storage.chm;

import static java.util.stream.Collectors.toList;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Storage implementation based on {@link ConcurrentHashMap}.
 */
public class TestConcurrentHashMapPartitionStorage implements PartitionStorage {
    /** Name of the snapshot file. */
    private static final String SNAPSHOT_FILE = "snapshot_file";

    /** Storage content. */
    private final ConcurrentSkipListMap<ByteArray, byte[]> map = new ConcurrentSkipListMap<>();

    private final int partId;

    /**
     * Constructor.
     *
     * @param partId Partition id.
     */
    public TestConcurrentHashMapPartitionStorage(int partId) {
        assert partId >= 0 : partId;

        this.partId = partId;
    }

    /** {@inheritDoc} */
    @Override
    public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public DataRow read(SearchRow key) throws StorageException {
        byte[] keyBytes = key.keyBytes();

        byte[] valueBytes = map.get(new ByteArray(keyBytes));

        return valueBytes == null ? null : new SimpleDataRow(keyBytes, valueBytes);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> readAll(List<? extends SearchRow> keys) {
        return keys.stream()
                .map(this::read)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    public void write(DataRow row) throws StorageException {
        map.put(new ByteArray(row.keyBytes()), row.valueBytes());
    }

    /** {@inheritDoc} */
    @Override
    public void writeAll(List<? extends DataRow> rows) throws StorageException {
        rows.forEach(this::write);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> insertAll(List<? extends DataRow> rows) throws StorageException {
        return rows.stream()
                .map(row -> map.putIfAbsent(new ByteArray(row.keyBytes()), row.valueBytes()) == null ? null : row)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    public void remove(SearchRow key) throws StorageException {
        map.remove(new ByteArray(key.keyBytes()));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<SearchRow> removeAll(List<? extends SearchRow> keys) {
        var skippedRows = new ArrayList<SearchRow>(keys.size());

        for (SearchRow key : keys) {
            byte[] keyBytes = key.keyBytes();

            byte[] removedValueBytes = map.remove(new ByteArray(keyBytes));

            if (removedValueBytes == null) {
                skippedRows.add(key);
            }
        }

        return skippedRows;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> removeAllExact(List<? extends DataRow> keyValues) {
        var skippedRows = new ArrayList<DataRow>(keyValues.size());

        for (DataRow row : keyValues) {
            var key = new ByteArray(row.keyBytes());

            byte[] existingValueBytes = map.get(key);

            if (Arrays.equals(existingValueBytes, row.valueBytes())) {
                map.remove(key);
            } else {
                skippedRows.add(row);
            }
        }

        return skippedRows;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public <T> T invoke(SearchRow key, InvokeClosure<T> clo) throws StorageException {
        byte[] keyBytes = key.keyBytes();

        ByteArray mapKey = new ByteArray(keyBytes);

        byte[] existingDataBytes = map.get(mapKey);

        clo.call(existingDataBytes == null ? null : new SimpleDataRow(keyBytes, existingDataBytes));

        switch (clo.operationType()) {
            case WRITE:
                DataRow newRow = clo.newRow();

                assert newRow != null;

                map.put(mapKey, newRow.valueBytes());

                break;

            case REMOVE:
                map.remove(mapKey);

                break;

            case NOOP:
                break;

            default:
                throw new UnsupportedOperationException(String.valueOf(clo.operationType()));
        }

        return clo.result();
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {
        Iterator<SimpleDataRow> iter = map.entrySet().stream()
                .map(e -> new SimpleDataRow(e.getKey().bytes(), e.getValue()))
                .filter(filter)
                .iterator();

        return Cursor.fromIterator(iter);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        return CompletableFuture.runAsync(() -> {
            try (
                    OutputStream out = Files.newOutputStream(snapshotPath.resolve(SNAPSHOT_FILE));
                    ObjectOutputStream objOut = new ObjectOutputStream(out)
            ) {
                objOut.writeObject(map.keySet().stream().map(ByteArray::bytes).collect(toList()));
                objOut.writeObject(new ArrayList<>(map.values()));
            } catch (Exception e) {
                throw new IgniteInternalException(e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void restoreSnapshot(Path snapshotPath) {
        try (
                InputStream in = Files.newInputStream(snapshotPath.resolve(SNAPSHOT_FILE));
                ObjectInputStream objIn = new ObjectInputStream(in)
        ) {
            var keys = (List<byte[]>) objIn.readObject();
            var values = (List<byte[]>) objIn.readObject();

            map.clear();

            for (int i = 0; i < keys.size(); i++) {
                map.put(new ByteArray(keys.get(i)), values.get(i));
            }
        } catch (Exception e) {
            throw new IgniteInternalException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        // No-op.
    }

    @Override
    public void destroy() {
        map.clear();
    }

    @Override
    public long rowsCount() {
        return map.size();
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

        TestConcurrentHashMapPartitionStorage that = (TestConcurrentHashMapPartitionStorage) o;

        if (!map.equals(that.map)) {
            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int hash = 0;

        for (Map.Entry<ByteArray, byte[]> entry : map.entrySet()) {
            hash += entry.getKey().hashCode() ^ Arrays.hashCode(entry.getValue());
        }

        return hash;
    }
}
