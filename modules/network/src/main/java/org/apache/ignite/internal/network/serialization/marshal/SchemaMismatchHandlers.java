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

package org.apache.ignite.internal.network.serialization.marshal;

import java.util.HashMap;
import java.util.Map;

/**
 * A colleciton of {@link SchemaMismatchHandler}s keyed by classes to which they relate.
 */
class SchemaMismatchHandlers {
    private final Map<Class<?>, SchemaMismatchHandler<?>> handlers = new HashMap<>();
    private final SchemaMismatchHandler<Object> defaultHandler = new DefaultSchemaMismatchHandler();

    <T> void registerHandler(Class<T> layerClass, SchemaMismatchHandler<T> handler) {
        handlers.put(layerClass, handler);
    }

    @SuppressWarnings("unchecked")
    private SchemaMismatchHandler<Object> handlerFor(Class<?> clazz) {
        SchemaMismatchHandler<Object> handler = (SchemaMismatchHandler<Object>) handlers.get(clazz);
        if (handler == null) {
            handler = defaultHandler;
        }
        return handler;
    }

    void onFieldIgnored(Class<?> layerClass, Object instance, String fieldName, Object fieldValue) throws SchemaMismatchException {
        handlerFor(layerClass).onFieldIgnored(instance, fieldName, fieldValue);
    }

    void onFieldMissed(Class<?> layerClass, Object instance, String fieldName) throws SchemaMismatchException {
        handlerFor(layerClass).onFieldMissed(instance, fieldName);
    }

    void onFieldTypeChanged(Class<?> layerClass, Object instance, String fieldName, Class<?> remoteType, Object fieldValue)
            throws SchemaMismatchException {
        handlerFor(layerClass).onFieldTypeChanged(instance, fieldName, remoteType, fieldValue);
    }
}
