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

import static java.lang.System.lineSeparator;
import static org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionConfigurationSchema.CLOCK_REPLACEMENT_MODE;
import static org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionConfigurationSchema.RANDOM_LRU_REPLACEMENT_MODE;
import static org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionConfigurationSchema.SEGMENTED_LRU_REPLACEMENT_MODE;
import static org.apache.ignite.internal.pagememory.io.PageIo.getCrc;
import static org.apache.ignite.internal.pagememory.io.PageIo.getPageId;
import static org.apache.ignite.internal.pagememory.io.PageIo.getType;
import static org.apache.ignite.internal.pagememory.io.PageIo.getVersion;
import static org.apache.ignite.internal.pagememory.io.PageIo.setPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.dirty;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.fullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.isAcquired;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.tempBufferPointer;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.writeTimestamp;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.effectivePageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.tag;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.compareAndSwapInt;
import static org.apache.ignite.internal.util.GridUnsafe.compareAndSwapLong;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.putIntVolatile;
import static org.apache.ignite.internal.util.GridUnsafe.setMemory;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;
import static org.apache.ignite.internal.util.IgniteUtils.hash;
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;
import static org.apache.ignite.internal.util.IgniteUtils.readableSize;
import static org.apache.ignite.internal.util.IgniteUtils.safeAbs;
import static org.apache.ignite.internal.util.IgniteUtils.toHexString;
import static org.apache.ignite.internal.util.OffheapReadWriteLock.TAG_LOCK_ALWAYS;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryProvider;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryRegion;
import org.apache.ignite.internal.pagememory.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Page header structure is described by the following diagram.
 *
 * <p>When page is not allocated (in a free list):
 * <pre>
 * +--------+------------------------------------------------------+
 * |8 bytes |         PAGE_SIZE + PAGE_OVERHEAD - 8 bytes          |
 * +--------+------------------------------------------------------+
 * |Next ptr|                      Page data                       |
 * +--------+------------------------------------------------------+
 * </pre>
 *
 * <p>When page is allocated and is in use:
 * <pre>
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * |     8 bytes      |8 bytes |8 bytes |4 b |4 b |8 bytes |8 bytes |       PAGE_SIZE      |
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * | Marker/Timestamp |Rel ptr |Page ID |C ID|PIN | LOCK   |TMP BUF |       Page data      |
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * </pre>
 *
 * <p>Note that first 8 bytes of page header are used either for page marker or for next relative pointer depending on whether the page is
 * in use or not.
 */
@SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
public class PageMemoryImpl implements PageMemoryEx {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(PageMemoryImpl.class);

    /** Full relative pointer mask. */
    public static final long RELATIVE_PTR_MASK = 0xFFFFFFFFFFFFFFL;

    /** Invalid relative pointer value. */
    static final long INVALID_REL_PTR = RELATIVE_PTR_MASK;

    /** Pointer which means that this page is outdated (for example, group was destroyed, partition eviction'd happened. */
    static final long OUTDATED_REL_PTR = INVALID_REL_PTR + 1;

    /** Page lock offset. */
    public static final int PAGE_LOCK_OFFSET = 32;

    /** 8b Marker/timestamp 8b Relative pointer 8b Page ID 4b Group ID 4b Pin count 8b Lock 8b Temporary buffer. */
    public static final int PAGE_OVERHEAD = 48;

    /** Try again tag. */
    public static final int TRY_AGAIN_TAG = -1;

    /** Data region configuration view. */
    private final PageMemoryDataRegionView dataRegionCfg;

    /** Page IO registry. */
    private final PageIoRegistry ioRegistry;

    /** Page manager. */
    private final PageReadWriteManager pmPageMgr;

    /** Page size. */
    private final int sysPageSize;

    /** Page replacement policy factory. */
    private final PageReplacementPolicyFactory pageReplacementPolicyFactory;

    /** Direct memory allocator. */
    private final DirectMemoryProvider directMemoryProvider;

    /** Segments array. */
    private volatile Segment[] segments;

    /** Lock for segments changes. */
    private final Object segmentsLock = new Object();

    /** Offheap read write lock instance. */
    private final OffheapReadWriteLock rwLock;

    /** Callback invoked to track changes in pages. {@code Null} if page tracking functionality is disabled. */
    @Nullable
    private final PageChangeTracker changeTracker;

    /** Field updater. */
    private static final AtomicIntegerFieldUpdater<PageMemoryImpl> pageReplacementWarnedFieldUpdater =
            AtomicIntegerFieldUpdater.newUpdater(PageMemoryImpl.class, "pageReplacementWarned");

    /** Flag indicating page replacement started (rotation with disk), allocating new page requires freeing old one. */
    private volatile int pageReplacementWarned;

    /** Segments sizes. */
    private final long[] sizes;

    /** {@code False} if memory was not started or already stopped and is not supposed for any usage. */
    private volatile boolean started;

    /**
     * Constructor.
     *
     * @param directMemoryProvider Memory allocator to use.
     * @param dataRegionCfg Data region configuration.
     * @param ioRegistry IO registry.
     * @param sizes Segments sizes.
     * @param pmPageMgr Page store manager.
     * @param changeTracker Callback invoked to track changes in pages.
     */
    public PageMemoryImpl(
            DirectMemoryProvider directMemoryProvider,
            PageMemoryDataRegionConfiguration dataRegionCfg,
            PageIoRegistry ioRegistry,
            long[] sizes,
            PageReadWriteManager pmPageMgr,
            @Nullable PageChangeTracker changeTracker
    ) {
        this.directMemoryProvider = directMemoryProvider;
        this.dataRegionCfg = dataRegionCfg.value();
        this.ioRegistry = ioRegistry;
        this.sizes = sizes;
        this.pmPageMgr = pmPageMgr;
        this.changeTracker = changeTracker;

        int pageSize = this.dataRegionCfg.pageSize();

        sysPageSize = pageSize + PAGE_OVERHEAD;

        rwLock = new OffheapReadWriteLock(128);

        String replacementMode = this.dataRegionCfg.replacementMode();

        switch (replacementMode) {
            case RANDOM_LRU_REPLACEMENT_MODE:
                pageReplacementPolicyFactory = new RandomLruPageReplacementPolicyFactory();

                break;
            case SEGMENTED_LRU_REPLACEMENT_MODE:
                pageReplacementPolicyFactory = new SegmentedLruPageReplacementPolicyFactory();

                break;
            case CLOCK_REPLACEMENT_MODE:
                pageReplacementPolicyFactory = new ClockPageReplacementPolicyFactory();

                break;
            default:
                throw new IgniteInternalException("Unexpected page replacement mode: " + replacementMode);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws IgniteInternalException {
        synchronized (segmentsLock) {
            if (started) {
                return;
            }

            started = true;

            directMemoryProvider.initialize(sizes);

            List<DirectMemoryRegion> regions = new ArrayList<>(sizes.length);

            while (true) {
                DirectMemoryRegion reg = directMemoryProvider.nextRegion();

                if (reg == null) {
                    break;
                }

                regions.add(reg);
            }

            int regs = regions.size();

            Segment[] segments = new Segment[regs - 1];

            DirectMemoryRegion cpReg = regions.get(regs - 1);

            long checkpointBuf = cpReg.size();

            long totalAllocated = 0;
            int pages = 0;
            long totalTblSize = 0;
            long totalReplSize = 0;

            for (int i = 0; i < regs - 1; i++) {
                assert i < segments.length;

                DirectMemoryRegion reg = regions.get(i);

                totalAllocated += reg.size();

                segments[i] = new Segment(i, regions.get(i));

                pages += segments[i].pages();
                totalTblSize += segments[i].tableSize();
                totalReplSize += segments[i].replacementSize();
            }

            this.segments = segments;

            if (LOG.isInfoEnabled()) {
                LOG.info("Started page memory [memoryAllocated=" + readableSize(totalAllocated, false)
                        + ", pages=" + pages
                        + ", tableSize=" + readableSize(totalTblSize, false)
                        + ", replacementSize=" + readableSize(totalReplSize, false)
                        + ", checkpointBuffer=" + readableSize(checkpointBuf, false)
                        + ']');
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop(boolean deallocate) throws IgniteInternalException {
        synchronized (segmentsLock) {
            if (!started) {
                return;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Stopping page memory.");
            }

            if (segments != null) {
                for (Segment seg : segments) {
                    seg.close();
                }
            }

            started = false;

            directMemoryProvider.shutdown(deallocate);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void releasePage(int grpId, long pageId, long page) {
        assert started;

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            seg.releasePage(page);
        } finally {
            seg.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public long readLock(int grpId, long pageId, long page) {
        assert started;

        return readLock(page, pageId, false);
    }

    /** {@inheritDoc} */
    @Override
    public long readLock(long absPtr, long pageId, boolean force, boolean touch) {
        assert started;

        int tag = force ? -1 : tag(pageId);

        boolean locked = rwLock.readLock(absPtr + PAGE_LOCK_OFFSET, tag);

        if (!locked) {
            return 0;
        }

        if (touch) {
            writeTimestamp(absPtr, coarseCurrentTimeMillis());
        }

        assert getCrc(absPtr + PAGE_OVERHEAD) == 0; //TODO IGNITE-16612

        return absPtr + PAGE_OVERHEAD;
    }

    private long readLock(long absPtr, long pageId, boolean force) {
        return readLock(absPtr, pageId, force, true);
    }

    /** {@inheritDoc} */
    @Override
    public void readUnlock(int grpId, long pageId, long page) {
        assert started;

        readUnlockPage(page);
    }

    /** {@inheritDoc} */
    @Override
    public long writeLock(int grpId, long pageId, long page) {
        assert started;

        return writeLock(grpId, pageId, page, false);
    }

    /** {@inheritDoc} */
    @Override
    public long writeLock(int grpId, long pageId, long page, boolean restore) {
        assert started;

        return writeLockPage(page, new FullPageId(pageId, grpId), !restore);
    }

    /** {@inheritDoc} */
    @Override
    public long tryWriteLock(int grpId, long pageId, long page) {
        assert started;

        return tryWriteLockPage(page, new FullPageId(pageId, grpId), true);
    }

    /** {@inheritDoc} */
    @Override
    public void writeUnlock(int grpId, long pageId, long page, boolean dirtyFlag) {
        assert started;

        writeUnlock(grpId, pageId, page, dirtyFlag, false);
    }

    /** {@inheritDoc} */
    @Override
    public void writeUnlock(int grpId, long pageId, long page, boolean dirtyFlag, boolean restore) {
        assert started;

        writeUnlockPage(page, new FullPageId(pageId, grpId), dirtyFlag, restore);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isDirty(int grpId, long pageId, long page) {
        assert started;

        return isDirty(page);
    }

    /**
     * Returns {@code true} if page is dirty.
     *
     * @param absPtr Absolute pointer.
     */
    boolean isDirty(long absPtr) {
        return dirty(absPtr);
    }

    /** {@inheritDoc} */
    @Override
    public long allocatePage(int grpId, int partId, byte flags) throws IgniteInternalCheckedException {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION && flags == FLAG_AUX : "flags = " + flags + ", partId = " + partId;

        assert started;

        long pageId = pmPageMgr.allocatePage(grpId, partId, flags);

        assert pageIndex(pageId) > 0; //it's crucial for tracking pages (zero page is super one)

        // We need to allocate page in memory for marking it dirty to save it in the next checkpoint.
        // Otherwise it is possible that on file will be empty page which will be saved at snapshot and read with error
        // because there is no crc inside them.
        Segment seg = segment(grpId, pageId);

        seg.writeLock().lock();

        FullPageId fullId = new FullPageId(pageId, grpId);

        try {
            long relPtr = seg.loadedPages.get(
                    grpId,
                    effectivePageId(pageId),
                    seg.partGeneration(grpId, partId),
                    INVALID_REL_PTR,
                    OUTDATED_REL_PTR
            );

            if (relPtr == OUTDATED_REL_PTR) {
                relPtr = seg.refreshOutdatedPage(grpId, pageId, false);

                seg.pageReplacementPolicy.onRemove(relPtr);
            }

            if (relPtr == INVALID_REL_PTR) {
                relPtr = seg.borrowOrAllocateFreePage(pageId);
            }

            if (relPtr == INVALID_REL_PTR) {
                relPtr = seg.removePageForReplacement();
            }

            long absPtr = seg.absolute(relPtr);

            setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte) 0);

            fullPageId(absPtr, fullId);
            writeTimestamp(absPtr, coarseCurrentTimeMillis());
            rwLock.init(absPtr + PAGE_LOCK_OFFSET, tag(pageId));

            assert getCrc(absPtr + PAGE_OVERHEAD) == 0; //TODO IGNITE-16612

            assert !isAcquired(absPtr) :
                    "Pin counter must be 0 for a new page [relPtr=" + hexLong(relPtr)
                            + ", absPtr=" + hexLong(absPtr) + ", pinCntr=" + PageHeader.pinCount(absPtr) + ']';

            setDirty(fullId, absPtr, true, true);

            seg.pageReplacementPolicy.onMiss(relPtr);

            seg.loadedPages.put(grpId, effectivePageId(pageId), relPtr, seg.partGeneration(grpId, partId));
        } catch (IgniteOutOfMemoryException oom) {
            IgniteOutOfMemoryException e = new IgniteOutOfMemoryException("Out of memory in data region ["
                    + "name=" + dataRegionCfg.name()
                    + ", initSize=" + readableSize(dataRegionCfg.initSize(), false)
                    + ", maxSize=" + readableSize(dataRegionCfg.maxSize(), false)
                    + ", persistenceEnabled=" + dataRegionCfg.persistent() + "] Try the following:" + lineSeparator()
                    + "  ^-- Increase maximum off-heap memory size (PageMemoryDataRegionConfiguration.maxSize)" + lineSeparator()
                    + "  ^-- Enable eviction or expiration policies"
            );

            e.initCause(oom);

            throw e;
        } finally {
            seg.writeLock().unlock();
        }

        return pageId;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer pageBuffer(long pageAddr) {
        return wrapPointer(pageAddr, pageSize());
    }

    /** {@inheritDoc} */
    @Override
    public boolean freePage(int grpId, long pageId) {
        assert false : "Free page should be never called directly when persistence is enabled.";

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public long partitionMetaPageId(int grpId, int partId) {
        assert started;

        //TODO IGNITE-16350 Consider reworking in FLAG_AUX.
        return pageId(partId, FLAG_DATA, 0);
    }

    /** {@inheritDoc} */
    @Override
    public long acquirePage(int grpId, long pageId) throws IgniteInternalCheckedException {
        return acquirePage(grpId, pageId, IoStatisticsHolderNoOp.INSTANCE, false);
    }

    /** {@inheritDoc} */
    @Override
    public long acquirePage(int grpId, long pageId, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
        assert started;

        return acquirePage(grpId, pageId, statHolder, false);
    }

    /** {@inheritDoc} */
    @Override
    public long acquirePage(int grpId, long pageId, AtomicBoolean pageAllocated) throws IgniteInternalCheckedException {
        return acquirePage(grpId, pageId, IoStatisticsHolderNoOp.INSTANCE, false, pageAllocated);
    }

    /** {@inheritDoc} */
    @Override
    public long acquirePage(int grpId, long pageId, IoStatisticsHolder statHolder, boolean restore) throws IgniteInternalCheckedException {
        return acquirePage(grpId, pageId, statHolder, restore, null);
    }

    private long acquirePage(
            int grpId,
            long pageId,
            IoStatisticsHolder statHolder,
            boolean restore,
            @Nullable AtomicBoolean pageAllocated
    ) throws IgniteInternalCheckedException {
        assert started;

        int partId = partitionId(pageId);

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            long relPtr = seg.loadedPages.get(
                    grpId,
                    effectivePageId(pageId),
                    seg.partGeneration(grpId, partId),
                    INVALID_REL_PTR,
                    INVALID_REL_PTR
            );

            // The page is loaded to the memory.
            if (relPtr != INVALID_REL_PTR) {
                long absPtr = seg.absolute(relPtr);

                seg.acquirePage(absPtr);

                seg.pageReplacementPolicy.onHit(relPtr);

                statHolder.trackLogicalRead(absPtr + PAGE_OVERHEAD);

                return absPtr;
            }
        } finally {
            seg.readLock().unlock();
        }

        FullPageId fullId = new FullPageId(pageId, grpId);

        seg.writeLock().lock();

        long lockedPageAbsPtr = -1;
        boolean readPageFromStore = false;

        try {
            // Double-check.
            long relPtr = seg.loadedPages.get(
                    grpId,
                    fullId.effectivePageId(),
                    seg.partGeneration(grpId, partId),
                    INVALID_REL_PTR,
                    OUTDATED_REL_PTR
            );

            long absPtr;

            if (relPtr == INVALID_REL_PTR) {
                relPtr = seg.borrowOrAllocateFreePage(pageId);

                if (pageAllocated != null) {
                    pageAllocated.set(true);
                }

                if (relPtr == INVALID_REL_PTR) {
                    relPtr = seg.removePageForReplacement();
                }

                absPtr = seg.absolute(relPtr);

                fullPageId(absPtr, fullId);
                writeTimestamp(absPtr, coarseCurrentTimeMillis());

                assert !isAcquired(absPtr) :
                        "Pin counter must be 0 for a new page [relPtr=" + hexLong(relPtr) + ", absPtr=" + hexLong(absPtr) + ']';

                // We can clear dirty flag after the page has been allocated.
                setDirty(fullId, absPtr, false, false);

                seg.pageReplacementPolicy.onMiss(relPtr);

                seg.loadedPages.put(
                        grpId,
                        fullId.effectivePageId(),
                        relPtr,
                        seg.partGeneration(grpId, partId)
                );

                long pageAddr = absPtr + PAGE_OVERHEAD;

                if (!restore) {
                    readPageFromStore = true;
                } else {
                    setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte) 0);

                    // Must init page ID in order to ensure RWLock tag consistency.
                    setPageId(pageAddr, pageId);
                }

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, tag(pageId));

                if (readPageFromStore) {
                    boolean locked = rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, TAG_LOCK_ALWAYS);

                    assert locked : "Page ID " + fullId + " expected to be locked";

                    lockedPageAbsPtr = absPtr;
                }
            } else if (relPtr == OUTDATED_REL_PTR) {
                assert pageIndex(pageId) == 0 : fullId;

                relPtr = seg.refreshOutdatedPage(grpId, pageId, false);

                absPtr = seg.absolute(relPtr);

                long pageAddr = absPtr + PAGE_OVERHEAD;

                setMemory(pageAddr, pageSize(), (byte) 0);

                fullPageId(absPtr, fullId);
                writeTimestamp(absPtr, coarseCurrentTimeMillis());
                setPageId(pageAddr, pageId);

                assert !isAcquired(absPtr) :
                        "Pin counter must be 0 for a new page [relPtr=" + hexLong(relPtr) + ", absPtr=" + hexLong(absPtr) + ']';

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, tag(pageId));

                seg.pageReplacementPolicy.onRemove(relPtr);
                seg.pageReplacementPolicy.onMiss(relPtr);
            } else {
                absPtr = seg.absolute(relPtr);

                seg.pageReplacementPolicy.onHit(relPtr);
            }

            seg.acquirePage(absPtr);

            if (!readPageFromStore) {
                statHolder.trackLogicalRead(absPtr + PAGE_OVERHEAD);
            }

            return absPtr;
        } finally {
            seg.writeLock().unlock();

            if (readPageFromStore) {
                assert lockedPageAbsPtr != -1 : "Page is expected to have a valid address [pageId=" + fullId
                        + ", lockedPageAbsPtr=" + hexLong(lockedPageAbsPtr) + ']';

                assert isPageWriteLocked(lockedPageAbsPtr) : "Page is expected to be locked: [pageId=" + fullId + "]";

                long pageAddr = lockedPageAbsPtr + PAGE_OVERHEAD;

                ByteBuffer buf = wrapPointer(pageAddr, pageSize());

                long actualPageId = 0;

                try {
                    pmPageMgr.read(grpId, pageId, buf, false);

                    statHolder.trackPhysicalAndLogicalRead(pageAddr);

                    actualPageId = getPageId(buf);
                } finally {
                    rwLock.writeUnlock(lockedPageAbsPtr + PAGE_LOCK_OFFSET, actualPageId == 0 ? TAG_LOCK_ALWAYS : tag(actualPageId));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public int pageSize() {
        return sysPageSize - PAGE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override
    public int systemPageSize() {
        return sysPageSize;
    }

    /** {@inheritDoc} */
    @Override
    public int realPageSize(int grpId) {
        return pageSize();
    }

    /**
     * Returns Max dirty ratio from the segments.
     */
    double getDirtyPagesRatio() {
        if (segments == null) {
            return 0;
        }

        double res = 0;

        for (Segment segment : segments) {
            res = Math.max(res, segment.getDirtyPagesRatio());
        }

        return res;
    }

    /**
     * Returns total pages can be placed in all segments.
     */
    @Override
    public long totalPages() {
        if (segments == null) {
            return 0;
        }

        long res = 0;

        for (Segment segment : segments) {
            res += segment.pages();
        }

        return res;
    }

    private void copyInBuffer(long absPtr, ByteBuffer tmpBuf) {
        if (tmpBuf.isDirect()) {
            long tmpPtr = bufferAddress(tmpBuf);

            copyMemory(absPtr + PAGE_OVERHEAD, tmpPtr, pageSize());

            assert getCrc(absPtr + PAGE_OVERHEAD) == 0; //TODO IGNITE-16612
            assert getCrc(tmpPtr) == 0; //TODO IGNITE-16612
        } else {
            byte[] arr = tmpBuf.array();

            assert arr.length == pageSize();

            copyMemory(null, absPtr + PAGE_OVERHEAD, arr, BYTE_ARR_OFF, pageSize());
        }
    }

    /**
     * Get current prartition generation tag.
     *
     * @param seg Segment.
     * @param fullId Full page id.
     * @return Current partition generation tag.
     */
    private int generationTag(Segment seg, FullPageId fullId) {
        return seg.partGeneration(
                fullId.groupId(),
                partitionId(fullId.pageId())
        );
    }

    /**
     * Resolver relative pointer via {@link LoadedPagesMap}.
     *
     * @param seg Segment.
     * @param fullId Full page id.
     * @param reqVer Required version.
     * @return Relative pointer.
     */
    private long resolveRelativePointer(Segment seg, FullPageId fullId, int reqVer) {
        return seg.loadedPages.get(
                fullId.groupId(),
                fullId.effectivePageId(),
                reqVer,
                INVALID_REL_PTR,
                OUTDATED_REL_PTR
        );
    }

    /** {@inheritDoc} */
    @Override
    public int invalidate(int grpId, int partId) {
        synchronized (segmentsLock) {
            if (!started) {
                return 0;
            }

            int tag = 0;

            for (Segment seg : segments) {
                seg.writeLock().lock();

                try {
                    int newTag = seg.incrementPartGeneration(grpId, partId);

                    if (tag == 0) {
                        tag = newTag;
                    }

                    assert tag == newTag;
                } finally {
                    seg.writeLock().unlock();
                }
            }

            return tag;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onCacheGroupDestroyed(int grpId) {
        for (Segment seg : segments) {
            seg.writeLock().lock();

            try {
                seg.resetGroupPartitionsGeneration(grpId);
            } finally {
                seg.writeLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public long loadedPages() {
        long total = 0;

        Segment[] segments = this.segments;

        if (segments != null) {
            for (Segment seg : segments) {
                if (seg == null) {
                    break;
                }

                seg.readLock().lock();

                try {
                    if (seg.closed) {
                        continue;
                    }

                    total += seg.loadedPages.size();
                } finally {
                    seg.readLock().unlock();
                }
            }
        }

        return total;
    }

    /**
     * Returns total number of acquired pages.
     */
    public long acquiredPages() {
        if (segments == null) {
            return 0L;
        }

        long total = 0;

        for (Segment seg : segments) {
            seg.readLock().lock();

            try {
                if (seg.closed) {
                    continue;
                }

                total += seg.acquiredPages();
            } finally {
                seg.readLock().unlock();
            }
        }

        return total;
    }

    /**
     * Returns {@code true} if the page is contained in the loaded pages table, {@code false} otherwise.
     *
     * @param fullPageId Full page ID to check.
     */
    public boolean hasLoadedPage(FullPageId fullPageId) {
        int grpId = fullPageId.groupId();
        long pageId = fullPageId.effectivePageId();
        int partId = partitionId(pageId);

        Segment seg = segment(grpId, pageId);

        seg.readLock().lock();

        try {
            long res = seg.loadedPages.get(grpId, pageId, seg.partGeneration(grpId, partId), INVALID_REL_PTR, INVALID_REL_PTR);

            return res != INVALID_REL_PTR;
        } finally {
            seg.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public long readLockForce(int grpId, long pageId, long page) {
        assert started;

        return readLock(page, pageId, true);
    }

    /**
     * Releases read lock.
     *
     * @param absPtr Absolute pointer to unlock.
     */
    void readUnlockPage(long absPtr) {
        rwLock.readUnlock(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * Checks if a page has temp copy buffer.
     *
     * @param absPtr Absolute pointer.
     * @return {@code True} if a page has temp buffer.
     */
    public boolean hasTempCopy(long absPtr) {
        return tempBufferPointer(absPtr) != INVALID_REL_PTR;
    }

    /**
     * Tries to acquire a write lock.
     *
     * @param absPtr Absolute pointer.
     * @return Pointer to the page write buffer or {@code 0} if page was not locked.
     */
    private long tryWriteLockPage(long absPtr, FullPageId fullId, boolean checkTag) {
        int tag = checkTag ? tag(fullId.pageId()) : TAG_LOCK_ALWAYS;

        return !rwLock.tryWriteLock(absPtr + PAGE_LOCK_OFFSET, tag) ? 0 : postWriteLockPage(absPtr, fullId);
    }

    /**
     * Acquires a write lock.
     *
     * @param absPtr Absolute pointer.
     * @return Pointer to the page write buffer or {@code 0} if page was not locked.
     */
    private long writeLockPage(long absPtr, FullPageId fullId, boolean checkTag) {
        int tag = checkTag ? tag(fullId.pageId()) : TAG_LOCK_ALWAYS;

        boolean locked = rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, tag);

        return locked ? postWriteLockPage(absPtr, fullId) : 0;
    }

    private long postWriteLockPage(long absPtr, FullPageId fullId) {
        writeTimestamp(absPtr, coarseCurrentTimeMillis());

        assert getCrc(absPtr + PAGE_OVERHEAD) == 0; //TODO IGNITE-16612

        return absPtr + PAGE_OVERHEAD;
    }

    private void writeUnlockPage(
            long page,
            FullPageId fullId,
            boolean markDirty,
            boolean restore
    ) {
        boolean wasDirty = isDirty(page);

        try {
            // If page is for restore, we shouldn't mark it as changed.
            if (!restore && markDirty && !wasDirty && changeTracker != null) {
                changeTracker.apply(page, fullId, this);
            }

            assert getCrc(page + PAGE_OVERHEAD) == 0; //TODO IGNITE-16612

            if (markDirty) {
                setDirty(fullId, page, true, false);
            }
        } finally { // Always release the lock.
            long pageId = getPageId(page + PAGE_OVERHEAD);

            try {
                assert pageId != 0 : hexLong(PageHeader.readPageId(page));

                rwLock.writeUnlock(page + PAGE_LOCK_OFFSET, tag(pageId));

                assert getVersion(page + PAGE_OVERHEAD) != 0 : dumpPage(pageId, fullId.groupId());
                assert getType(page + PAGE_OVERHEAD) != 0 : hexLong(pageId);
            } catch (AssertionError ex) {
                LOG.error("Failed to unlock page [fullPageId=" + fullId + ", binPage=" + toHexString(page, systemPageSize()) + ']');

                throw ex;
            }
        }
    }

    /**
     * Prepares page details for assertion.
     *
     * @param pageId Page id.
     * @param grpId Group id.
     */
    private String dumpPage(long pageId, int grpId) {
        int pageIdx = pageIndex(pageId);
        int partId = partitionId(pageId);
        long off = (long) (pageIdx + 1) * pageSize();

        return hexLong(pageId) + " (grpId=" + grpId + ", pageIdx=" + pageIdx + ", partId=" + partId + ", offH="
                + Long.toHexString(off) + ")";
    }

    /**
     * Returns {@code true} if write lock acquired for the page.
     *
     * @param absPtr Absolute pointer to the page.
     */
    boolean isPageWriteLocked(long absPtr) {
        return rwLock.isWriteLocked(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * Returns {@code true} if read lock acquired for the page.
     *
     * @param absPtr Absolute pointer to the page.
     */
    boolean isPageReadLocked(long absPtr) {
        return rwLock.isReadLocked(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * Returns the number of active pages across all segments. Used for test purposes only.
     */
    public int activePagesCount() {
        if (segments == null) {
            return 0;
        }

        int total = 0;

        for (Segment seg : segments) {
            total += seg.acquiredPages();
        }

        return total;
    }

    /**
     * This method must be called in synchronized context.
     *
     * @param pageId full page ID.
     * @param absPtr Absolute pointer.
     * @param dirty {@code True} dirty flag.
     * @param forceAdd If this flag is {@code true}, then the page will be added to the dirty set regardless whether the old flag was dirty
     *      or not.
     */
    private void setDirty(FullPageId pageId, long absPtr, boolean dirty, boolean forceAdd) {
        boolean wasDirty = dirty(absPtr, dirty);

        if (dirty) {
            if (!wasDirty || forceAdd) {
                Segment seg = segment(pageId.groupId(), pageId.pageId());

                if (seg.dirtyPages.add(pageId)) {
                    seg.dirtyPagesCntr.incrementAndGet();
                }
            }
        } else {
            Segment seg = segment(pageId.groupId(), pageId.pageId());

            if (seg.dirtyPages.remove(pageId)) {
                seg.dirtyPagesCntr.decrementAndGet();
            }
        }
    }

    private Segment segment(int grpId, long pageId) {
        int idx = segmentIndex(grpId, pageId, segments.length);

        return segments[idx];
    }

    private static int segmentIndex(int grpId, long pageId, int segments) {
        pageId = effectivePageId(pageId);

        // Take a prime number larger than total number of partitions.
        int hash = hash(pageId * 65537 + grpId);

        return safeAbs(hash) % segments;
    }

    /**
     * Returns a collection of all pages currently marked as dirty. Will create a collection copy.
     */
    @TestOnly
    public Collection<FullPageId> dirtyPages() {
        if (segments == null) {
            return Collections.emptySet();
        }

        Collection<FullPageId> res = new HashSet<>((int) loadedPages());

        for (Segment seg : segments) {
            res.addAll(seg.dirtyPages);
        }

        return res;
    }

    /**
     * Page segment.
     */
    class Segment extends ReentrantReadWriteLock {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Pointer to acquired pages integer counter. */
        private static final int ACQUIRED_PAGES_SIZEOF = 4;

        /** Padding to read from word beginning. */
        private static final int ACQUIRED_PAGES_PADDING = 4;

        /** Page ID to relative pointer map. */
        private final LoadedPagesMap loadedPages;

        /** Pointer to acquired pages integer counter. */
        private final long acquiredPagesPtr;

        /** Page pool. */
        private final PagePool pool;

        /** Page replacement policy. */
        private final PageReplacementPolicy pageReplacementPolicy;

        /** Bytes required to store {@link #loadedPages}. */
        private final long memPerTbl;

        /** Bytes required to store {@link #pageReplacementPolicy} service data. */
        private long memPerRepl;

        /** Pages marked as dirty since the last checkpoint. */
        private volatile Collection<FullPageId> dirtyPages = ConcurrentHashMap.newKeySet();

        /** Atomic size counter for {@link #dirtyPages}. */
        private final AtomicLong dirtyPagesCntr = new AtomicLong();

        /** Maximum number of dirty pages. */
        private final long maxDirtyPages;

        /** Initial partition generation. */
        private static final int INIT_PART_GENERATION = 1;

        /** Maps partition (grpId, partId) to its generation. Generation is 1-based incrementing partition counter. */
        private final Map<GroupPartitionId, Integer> partGenerationMap = new HashMap<>();

        /** Segment closed flag. */
        private boolean closed;

        /**
         * Constructor.
         *
         * @param idx Segment index.
         * @param region Memory region.
         */
        private Segment(int idx, DirectMemoryRegion region) {
            long totalMemory = region.size();

            int pages = (int) (totalMemory / sysPageSize);

            acquiredPagesPtr = region.address();

            putIntVolatile(null, acquiredPagesPtr, 0);

            int ldPagesMapOffInRegion = ACQUIRED_PAGES_SIZEOF + ACQUIRED_PAGES_PADDING;

            long ldPagesAddr = region.address() + ldPagesMapOffInRegion;

            memPerTbl = RobinHoodBackwardShiftHashMap.requiredMemory(pages);

            loadedPages = new RobinHoodBackwardShiftHashMap(ldPagesAddr, memPerTbl);

            pages = (int) ((totalMemory - memPerTbl - ldPagesMapOffInRegion) / sysPageSize);

            memPerRepl = pageReplacementPolicyFactory.requiredMemory(pages);

            DirectMemoryRegion poolRegion = region.slice(memPerTbl + memPerRepl + ldPagesMapOffInRegion);

            pool = new PagePool(idx, poolRegion, sysPageSize, rwLock);

            pageReplacementPolicy = pageReplacementPolicyFactory.create(
                    this,
                    region.address() + memPerTbl + ldPagesMapOffInRegion,
                    pool.pages()
            );

            maxDirtyPages = pool.pages() * 3L / 4;
        }

        /**
         * Closes the segment.
         */
        private void close() {
            writeLock().lock();

            try {
                closed = true;
            } finally {
                writeLock().unlock();
            }
        }

        /**
         * Returns dirtyRatio to be compared with Throttle threshold.
         */
        private double getDirtyPagesRatio() {
            return dirtyPagesCntr.doubleValue() / pages();
        }

        /**
         * Returns max number of pages this segment can allocate.
         */
        private int pages() {
            return pool.pages();
        }

        /**
         * Returns memory allocated for pages table.
         */
        private long tableSize() {
            return memPerTbl;
        }

        /**
         * Returns memory allocated for page replacement service data.
         */
        private long replacementSize() {
            return memPerRepl;
        }

        private void acquirePage(long absPtr) {
            PageHeader.acquirePage(absPtr);

            updateAtomicInt(acquiredPagesPtr, 1);
        }

        private void releasePage(long absPtr) {
            PageHeader.releasePage(absPtr);

            updateAtomicInt(acquiredPagesPtr, -1);
        }

        /**
         * Returns total number of acquired pages.
         */
        private int acquiredPages() {
            return getInt(acquiredPagesPtr);
        }

        /**
         * Allocates a new free page.
         *
         * @param pageId Page ID.
         * @return Page relative pointer.
         */
        private long borrowOrAllocateFreePage(long pageId) {
            return pool.borrowOrAllocateFreePage(tag(pageId));
        }

        /**
         * Clear dirty pages collection and reset counter.
         */
        private void resetDirtyPages() {
            dirtyPages = ConcurrentHashMap.newKeySet();

            dirtyPagesCntr.set(0);
        }

        /**
         * Prepares a page removal for page replacement, if needed.
         *
         * @param fullPageId Candidate page full ID.
         * @param absPtr Absolute pointer of the page to evict.
         * @return {@code True} if it is ok to replace this page, {@code false} if another page should be selected.
         * @throws IgniteInternalCheckedException If failed to write page to the underlying store during eviction.
         */
        public boolean tryToRemovePage(FullPageId fullPageId, long absPtr) throws IgniteInternalCheckedException {
            assert writeLock().isHeldByCurrentThread();

            // Do not evict group meta pages.
            if (fullPageId.pageId() == META_PAGE_ID) {
                return false;
            }

            if (isAcquired(absPtr)) {
                return false;
            }

            if (isDirty(absPtr)) {
                return false;
            }

            loadedPages.remove(fullPageId.groupId(), fullPageId.effectivePageId());

            return true;
        }

        /**
         * Refresh outdated value.
         *
         * @param grpId Group ID.
         * @param pageId Page ID.
         * @param rmv {@code True} if page should be removed.
         * @return Relative pointer to refreshed page.
         */
        public long refreshOutdatedPage(int grpId, long pageId, boolean rmv) {
            assert writeLock().isHeldByCurrentThread();

            int tag = partGeneration(grpId, partitionId(pageId));

            long relPtr = loadedPages.refresh(grpId, effectivePageId(pageId), tag);

            long absPtr = absolute(relPtr);

            setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte) 0);

            dirty(absPtr, false);

            if (rmv) {
                loadedPages.remove(grpId, effectivePageId(pageId));
            }

            Collection<FullPageId> dirtyPages = this.dirtyPages;

            if (dirtyPages != null) {
                if (dirtyPages.remove(new FullPageId(pageId, grpId))) {
                    dirtyPagesCntr.decrementAndGet();
                }
            }

            return relPtr;
        }

        /**
         * Removes random oldest page for page replacement from memory to storage.
         *
         * @return Relative address for removed page, now it can be replaced by allocated or reloaded page.
         * @throws IgniteInternalCheckedException If failed to evict page.
         */
        private long removePageForReplacement() throws IgniteInternalCheckedException {
            assert getWriteHoldCount() > 0;

            if (pageReplacementWarned == 0) {
                if (pageReplacementWarnedFieldUpdater.compareAndSet(PageMemoryImpl.this, 0, 1)) {
                    String msg = "Page replacements started, pages will be rotated with disk, this will affect "
                            + "storage performance (consider increasing PageMemoryDataRegionConfiguration#setMaxSize for "
                            + "data region): " + dataRegionCfg.name();

                    LOG.warn(msg);
                }
            }

            if (acquiredPages() >= loadedPages.size()) {
                throw oomException("all pages are acquired");
            }

            return pageReplacementPolicy.replace();
        }

        /**
         * Creates out of memory exception with additional information.
         *
         * @param reason Reason.
         */
        public IgniteOutOfMemoryException oomException(String reason) {
            return new IgniteOutOfMemoryException("Failed to find a page for eviction (" + reason + ") ["
                    + "segmentCapacity=" + loadedPages.capacity()
                    + ", loaded=" + loadedPages.size()
                    + ", maxDirtyPages=" + maxDirtyPages
                    + ", dirtyPages=" + dirtyPagesCntr
                    + ", pinned=" + acquiredPages()
                    + ']' + lineSeparator() + "Out of memory in data region ["
                    + "name=" + dataRegionCfg.name()
                    + ", initSize=" + readableSize(dataRegionCfg.initSize(), false)
                    + ", maxSize=" + readableSize(dataRegionCfg.maxSize(), false)
                    + ", persistenceEnabled=" + dataRegionCfg.persistent() + "] Try the following:" + lineSeparator()
                    + "  ^-- Increase maximum off-heap memory size (PageMemoryDataRegionConfiguration.maxSize)" + lineSeparator()
                    + "  ^-- Enable eviction or expiration policies"
            );
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param relPtr Relative pointer.
         * @return Absolute pointer.
         */
        public long absolute(long relPtr) {
            return pool.absolute(relPtr);
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param pageIdx Page index.
         * @return Relative pointer.
         */
        public long relative(long pageIdx) {
            return pool.relative(pageIdx);
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param relPtr Relative pointer.
         * @return Page index in the pool.
         */
        public long pageIndex(long relPtr) {
            return pool.pageIndex(relPtr);
        }

        /**
         * Returns partition generation. Growing, 1-based partition version.
         *
         * @param grpId Group ID.
         * @param partId Partition ID.
         */
        public int partGeneration(int grpId, int partId) {
            assert getReadHoldCount() > 0 || getWriteHoldCount() > 0;

            Integer tag = partGenerationMap.get(new GroupPartitionId(grpId, partId));

            assert tag == null || tag >= 0 : "Negative tag=" + tag;

            return tag == null ? INIT_PART_GENERATION : tag;
        }

        /**
         * Gets loaded pages map.
         */
        public LoadedPagesMap loadedPages() {
            return loadedPages;
        }

        /**
         * Gets page pool.
         */
        public PagePool pool() {
            return pool;
        }

        /**
         * Increments partition generation due to partition invalidation (e.g. partition was rebalanced to other node and evicted).
         *
         * @param grpId Group ID.
         * @param partId Partition ID.
         * @return New partition generation.
         */
        private int incrementPartGeneration(int grpId, int partId) {
            assert getWriteHoldCount() > 0;

            GroupPartitionId grpPart = new GroupPartitionId(grpId, partId);

            Integer gen = partGenerationMap.get(grpPart);

            if (gen == null) {
                gen = INIT_PART_GENERATION;
            }

            if (gen == Integer.MAX_VALUE) {
                LOG.warn("Partition tag overflow [grpId=" + grpId + ", partId=" + partId + "]");

                partGenerationMap.put(grpPart, 0);

                return 0;
            } else {
                partGenerationMap.put(grpPart, gen + 1);

                return gen + 1;
            }
        }

        private void resetGroupPartitionsGeneration(int grpId) {
            assert getWriteHoldCount() > 0;

            partGenerationMap.keySet().removeIf(grpPart -> grpPart.getGroupId() == grpId);
        }

        /**
         * Returns IO registry.
         */
        PageIoRegistry ioRegistry() {
            return ioRegistry;
        }
    }

    private static int updateAtomicInt(long ptr, int delta) {
        while (true) {
            int old = getInt(ptr);

            int updated = old + delta;

            if (compareAndSwapInt(null, ptr, old, updated)) {
                return updated;
            }
        }
    }

    private static long updateAtomicLong(long ptr, long delta) {
        while (true) {
            long old = GridUnsafe.getLong(ptr);

            long updated = old + delta;

            if (compareAndSwapLong(null, ptr, old, updated)) {
                return updated;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public PageIoRegistry ioRegistry() {
        return ioRegistry;
    }

    /**
     * Callback invoked to track changes in pages.
     */
    @FunctionalInterface
    public interface PageChangeTracker {
        /**
         * Callback body.
         *
         * @param page – Page pointer.
         * @param fullPageId Full page ID.
         * @param pageMemoryEx Page memory.
         */
        void apply(long page, FullPageId fullPageId, PageMemoryEx pageMemoryEx);
    }
}
