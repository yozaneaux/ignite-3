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

package org.apache.ignite.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Key-Value view of table provides methods to access table records.
 *
 * @param <K> Mapped key type.
 * @param <V> Mapped value type.
 * @apiNote 'Key/value class field' &gt;-&lt; 'table column' mapping laid down in implementation.
 * @see org.apache.ignite.table.mapper.Mapper
 */
public interface KeyValueView<K, V> {
    /**
     * Gets a value associated with the given key.
     *
     * <p>Note: If the value mapper implies a value can be {@code null}, then a suitable method
     * {@link #getNullable(Transaction, Object)} must be used instead.
     *
     * @param tx  The transaction or {@code null} to auto commit. 
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.     
     * @return Value or {@code null}, if it does not exist.
     * @throws IllegalStateException If value for the key exists, and it is {@code null}.
     * @see #getNullable(Transaction, Object)
     */
    V get(@Nullable Transaction tx, @NotNull K key);

    /**
     * Asynchronously gets a value associated with the given key.
     *
     * <p>Note: If the value mapper implies a value can be {@code null}, then a suitable method
     * {@link #getNullableAsync(Transaction, Object)} must be used instead.
     *
     * @param tx  The transaction or {@code null} to auto commit. 
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     * @see #getNullableAsync(Transaction, Object)
     * @see #get(Transaction, Object)
     */
    @NotNull CompletableFuture<V> getAsync(@Nullable Transaction tx, @NotNull K key);

    /**
     * Gets a nullable value associated with the given key.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @return Wrapped nullable value or {@code null}, if it does not exist.
     */
    default NullableValue<V> getNullable(@Nullable Transaction tx, K key) {
        //TODO: to be implemented https://issues.apache.org/jira/browse/IGNITE-16115
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Gets a nullable value associated with the given key.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     * @see #getNullable(Transaction, Object)
     */
    default @NotNull CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, K key) {
        //TODO: to be implemented https://issues.apache.org/jira/browse/IGNITE-16115
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Gets a value associated with the given key if exists, returns {@code defaultValue} otherwise.
     *
     * <p>Note: method has same semantic as {@link #get(Transaction, Object)} with regard to {@code null} values.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @return Value or {@code defaultValue}, if does not exist.
     * @throws IllegalStateException If value for the key exists, and it is {@code null}.
     */
    default V getOrDefault(@Nullable Transaction tx, K key, V defaultValue) {
        //TODO: to be implemented https://issues.apache.org/jira/browse/IGNITE-16115
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Gets a nullable value associated with the given key.
     *
     * <p>Note: method has same semantic as {@link #get(Transaction, Object)} with regard to {@code null} values.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     * @see #getOrDefault(Transaction, Object, Object)
     */
    default @NotNull CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, K key, V defaultValue) {
        //TODO: to be implemented https://issues.apache.org/jira/browse/IGNITE-16115
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Get values associated with given keys.
     *
     * @param tx   The transaction or {@code null} to auto commit.
     * @param keys Keys which associated values are to be returned. The keys cannot be {@code null}.
     * @return Values associated with given keys.
     */
    Map<K, V> getAll(@Nullable Transaction tx, @NotNull Collection<K> keys);

    /**
     * Get values associated with given keys.
     *
     * @param tx   The transaction or {@code null} to auto commit.
     * @param keys Keys whose associated values are to be returned. The keys cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, @NotNull Collection<K> keys);

    /**
     * Determines if the table contains an entry for the specified key.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which presence is to be tested. The key cannot be {@code null}.
     * @return {@code True} if a value exists for the specified key, {@code false} otherwise.
     */
    boolean contains(@Nullable Transaction tx, @NotNull K key);

    /**
     * Determines if the table contains an entry for the specified key.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which presence is to be tested. The key cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, @NotNull K key);

    /**
     * Puts value associated with given key into the table.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     */
    void put(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Asynchronously puts value associated with given key into the table.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> putAsync(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Put associated key-value pairs.
     *
     * @param tx    The transaction or {@code null} to auto commit.
     * @param pairs Key-value pairs. The pairs cannot be {@code null}.
     */
    void putAll(@Nullable Transaction tx, @NotNull Map<K, V> pairs);

    /**
     * Asynchronously put associated key-value pairs.
     *
     * @param tx    The transaction or {@code null} to auto commit.
     * @param pairs Key-value pairs. The pairs cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, @NotNull Map<K, V> pairs);

    /**
     * Puts new or replaces existed value associated with given key into the table.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Replaced value or {@code null}, if not existed.
     */
    V getAndPut(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Asynchronously puts new or replaces existed value associated with given key into the table.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Puts value associated with given key into the table if not exists.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return {@code True} if successful, {@code false} otherwise.
     */
    boolean putIfAbsent(@Nullable Transaction tx, @NotNull K key, @NotNull V val);

    /**
     * Asynchronously puts value associated with given key into the table if not exists.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Removes value associated with given key from the table.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which mapping is to be removed from the table. The key cannot be {@code null}.
     * @return {@code True} if a value associated with the specified key was successfully removed, {@code false} otherwise.
     */
    boolean remove(@Nullable Transaction tx, @NotNull K key);

    /**
     * Removes an expected value associated with the given key from the table.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which associated value is to be removed from the table. The key cannot be {@code null}.
     * @param val Expected value.
     * @return {@code True} if the expected value for the specified key was successfully removed, {@code false} otherwise.
     */
    boolean remove(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Asynchronously removes value associated with given key from the table.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which mapping is to be removed from the table. The key cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull K key);

    /**
     * Asynchronously removes expected value associated with given key from the table.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which associated the value is to be removed from the table. The key cannot be {@code null}.
     * @param val Expected value.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Remove values associated with given keys from the table.
     *
     * @param tx   The transaction or {@code null} to auto commit.
     * @param keys Keys which mapping is to be removed from the table. The keys cannot be {@code null}.
     * @return Keys which did not exist.
     */
    Collection<K> removeAll(@Nullable Transaction tx, @NotNull Collection<K> keys);

    /**
     * Asynchronously remove values associated with given keys from the table.
     *
     * @param tx   The transaction or {@code null} to auto commit.
     * @param keys Keys which mapping is to be removed from the table. The keys cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, @NotNull Collection<K> keys);

    /**
     * Gets then removes value associated with given key from the table.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key which associated value is to be removed from the table. The key cannot be {@code null}.
     * @return Removed value or {@code null}, if not existed.
     */
    V getAndRemove(@Nullable Transaction tx, @NotNull K key);

    /**
     * Asynchronously gets then removes value associated with given key from the table.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A Key which mapping is to be removed from the table. The key cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, @NotNull K key);

    /**
     * Replaces the value for a key only if exists. This is equivalent to
     * <pre><code>
     * if (cache.containsKey(tx, key)) {
     *   cache.put(tx, key, value);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key with which the specified value is associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     */
    boolean replace(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Replaces the expected value for a key. This is equivalent to
     * <pre><code>
     * if (cache.get(tx, key) == oldVal) {
     *   cache.put(tx, key, newVal);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param tx     The transaction or {@code null} to auto commit.
     * @param key    A key with which the specified value is associated. The key cannot be {@code null}.
     * @param oldVal Expected value associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     */
    boolean replace(@Nullable Transaction tx, @NotNull K key, V oldVal, V newVal);

    /**
     * Asynchronously replaces the value for a key only if exists. See {@link #replace(Transaction, Object, Object)}.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key with which the specified value is associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Asynchronously replaces the expected value for a key. See {@link #replace(Transaction, Object, Object, Object)}
     *
     * @param tx     The transaction or {@code null} to auto commit.
     * @param key    A key with which the specified value is associated. The key cannot be {@code null}.
     * @param oldVal Expected value associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull K key, V oldVal, V newVal);

    /**
     * Replaces the value for a given key only if exists. This is equivalent to
     * <pre><code>
     * if (cache.containsKey(tx, key)) {
     *   V oldValue = cache.get(tx, key);
     *   cache.put(tx, key, value);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key with which the specified value is associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Replaced value, or {@code null} if not existed.
     */
    V getAndReplace(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Asynchronously replaces the value for a given key only if exists. See {@link #getAndReplace(Transaction, Object, Object)}
     *
     * @param tx  The transaction or {@code null} to auto commit.
     * @param key A key with which the specified value is associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, @NotNull K key, V val);

    /**
     * Executes invoke processor code against the value associated with the provided key.
     *
     * @param tx   The transaction or {@code null} to auto commit.
     * @param key  A key associated with the value that invoke processor will be applied to. The key cannot be {@code null}.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @param <R>  Invoke processor result type.
     * @return Result of the processing.
     * @see InvokeProcessor
     */
    <R extends Serializable> R invoke(@Nullable Transaction tx, @NotNull K key, InvokeProcessor<K, V, R> proc, Serializable... args);

    /**
     * Asynchronously executes invoke processor code against the value associated with the provided key.
     *
     * @param tx   The transaction or {@code null} to auto commit.
     * @param key  A key associated with the value that invoke processor will be applied to. The key cannot be {@code null}.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @param <R>  Invoke processor result type.
     * @return Future representing pending completion of the operation.
     * @see InvokeProcessor
     */
    @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
            @Nullable Transaction tx,
            @NotNull K key,
            InvokeProcessor<K, V, R> proc,
            Serializable... args);

    /**
     * Executes invoke processor code against values associated with the provided keys.
     *
     * @param tx   The transaction or {@code null} to auto commit.
     * @param <R>  Invoke processor result type.
     * @param keys Ordered collection of keys which values associated with should be processed. The keys cannot be {@code null}.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @return Results of the processing.
     * @see InvokeProcessor
     */
    <R extends Serializable> Map<K, R> invokeAll(
            @Nullable Transaction tx,
            @NotNull Collection<K> keys,
            InvokeProcessor<K, V, R> proc,
            Serializable... args);

    /**
     * Asynchronously executes invoke processor code against values associated with the provided keys.
     *
     * @param tx   The transaction or {@code null} to auto commit.
     * @param <R>  Invoke processor result type.
     * @param keys Ordered collection of keys which values associated with should be processed. The keys cannot be {@code null}.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @return Future representing pending completion of the operation.
     * @see InvokeProcessor
     */
    @NotNull <R extends Serializable> CompletableFuture<Map<K, R>> invokeAllAsync(
            @Nullable Transaction tx,
            @NotNull Collection<K> keys,
            InvokeProcessor<K, V, R> proc,
            Serializable... args);
}
