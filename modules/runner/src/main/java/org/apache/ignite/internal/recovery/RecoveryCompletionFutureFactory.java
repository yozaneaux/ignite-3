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

package org.apache.ignite.internal.recovery;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListener;
import org.apache.ignite.internal.metastorage.MetaStorageManager;

/**
 * Creates a future that completes when local recovery is finished.
 */
public class RecoveryCompletionFutureFactory extends CompletableFuture<Void> {
    /**
     * Create recovery completion future.
     *
     * @param metaStorageMgr Metastorage manager.
     * @param clusterCfgMgr Cluster configuration manager.
     * @param listenerProvider Provider of configuration listener.
     * @return Recovery completion future.
     */
    public static CompletableFuture<Void> create(
            MetaStorageManager metaStorageMgr,
            ConfigurationManager clusterCfgMgr,
            Function<CompletableFuture<Void>, ConfigurationStorageRevisionListener> listenerProvider
    ) {
        //TODO: IGNITE-15114 This is a temporary solution until full process of the node join is implemented.
        if (!metaStorageMgr.isMetaStorageInitializedOnStart()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> configCatchUpFuture = new CompletableFuture<>();

        ConfigurationStorageRevisionListener listener = listenerProvider.apply(configCatchUpFuture);

        CompletableFuture<Void> recoveryCompletionFuture =
                configCatchUpFuture.thenRun(() -> clusterCfgMgr.configurationRegistry().stopListenUpdateStorageRevision(listener));

        clusterCfgMgr.configurationRegistry().listenUpdateStorageRevision(listener);

        return recoveryCompletionFuture;
    }
}
