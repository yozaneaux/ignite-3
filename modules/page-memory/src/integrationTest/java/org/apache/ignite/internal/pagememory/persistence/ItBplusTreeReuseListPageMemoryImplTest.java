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

package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.util.Constants.MiB;

import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagememory.tree.ItBplusTreeReuseSelfTest;

/**
 * Test with reuse list and {@link PageMemoryImpl}.
 */
public class ItBplusTreeReuseListPageMemoryImplTest extends ItBplusTreeReuseSelfTest {
    /** {@inheritDoc} */
    @Override
    protected PageMemory createPageMemory() throws Exception {
        dataRegionCfg
                .change(c -> c.changePageSize(PAGE_SIZE).changeInitSize(MAX_MEMORY_SIZE).changeMaxSize(MAX_MEMORY_SIZE))
                .get(1, TimeUnit.SECONDS);

        long[] sizes = LongStream.range(0, CPUS + 1).map(i -> MAX_MEMORY_SIZE / CPUS).toArray();

        sizes[CPUS] = 10 * MiB;

        TestPageIoRegistry ioRegistry = new TestPageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PageMemoryImpl(
                new UnsafeMemoryProvider(null),
                dataRegionCfg,
                ioRegistry,
                sizes,
                new TestPageReadWriteManager(),
                (page, fullPageId, pageMemoryEx) -> {
                }
        );
    }

    /** {@inheritDoc} */
    @Override
    protected long acquiredPages() {
        return ((PageMemoryImpl) pageMem).acquiredPages();
    }
}
