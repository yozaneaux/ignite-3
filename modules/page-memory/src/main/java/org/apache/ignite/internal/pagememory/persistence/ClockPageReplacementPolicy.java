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

import static org.apache.ignite.internal.pagememory.persistence.PageHeader.fullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl.INVALID_REL_PTR;
import static org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl.OUTDATED_REL_PTR;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;

import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl.Segment;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * CLOCK page replacement policy implementation.
 */
public class ClockPageReplacementPolicy extends PageReplacementPolicy {
    /** Pages hit-flags store. */
    private final ClockPageReplacementFlags flags;

    /**
     * Constructor.
     *
     * @param seg Page memory segment.
     * @param ptr Pointer to memory region.
     * @param pagesCnt Pages count.
     */
    protected ClockPageReplacementPolicy(Segment seg, long ptr, int pagesCnt) {
        super(seg);

        flags = new ClockPageReplacementFlags(pagesCnt, ptr);
    }

    /** {@inheritDoc} */
    @Override
    public void onHit(long relPtr) {
        int pageIdx = (int) seg.pageIndex(relPtr);

        flags.setFlag(pageIdx);
    }

    /** {@inheritDoc} */
    @Override
    public void onRemove(long relPtr) {
        int pageIdx = (int) seg.pageIndex(relPtr);

        flags.clearFlag(pageIdx);
    }

    /** {@inheritDoc} */
    @Override
    public long replace() throws IgniteInternalCheckedException {
        LoadedPagesMap loadedPages = seg.loadedPages();

        for (int i = 0; i < loadedPages.size(); i++) {
            int pageIdx = flags.poll();

            long relPtr = seg.relative(pageIdx);
            long absPtr = seg.absolute(relPtr);

            FullPageId fullId = fullPageId(absPtr);

            // Check loaded pages map for outdated page.
            relPtr = loadedPages.get(
                    fullId.groupId(),
                    fullId.effectivePageId(),
                    seg.partGeneration(fullId.groupId(), partitionId(fullId.pageId())),
                    INVALID_REL_PTR,
                    OUTDATED_REL_PTR
            );

            assert relPtr != INVALID_REL_PTR;

            if (relPtr == OUTDATED_REL_PTR) {
                return seg.refreshOutdatedPage(fullId.groupId(), fullId.pageId(), true);
            }

            if (seg.tryToRemovePage(fullId, absPtr)) {
                return relPtr;
            }

            flags.setFlag(pageIdx);
        }

        throw seg.oomException("no pages to replace");
    }
}
