package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;

public class BufferMgr {
    private Buffer[] bufferPool;
    private int numAvailable;  // number of available (i.e. unpinned) buffers
    private static final long MAX_TIME = 10000; // 10s

    public BufferMgr(FileMgr fm, LogMgr lm, int poolSize) {
        bufferPool = new Buffer[poolSize];
        numAvailable = poolSize;
        for (int i = 0; i < poolSize; i++) {
            bufferPool[i] = new Buffer(fm, lm);
        }
    }

    public synchronized int available() {
        return numAvailable;
    }

    public synchronized void flushAll(int txnum) {
        for (Buffer buffer: bufferPool) {
            if (buffer.modifyingTx() == txnum)
                buffer.flush();
        }
    }

    public synchronized void unpin(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            numAvailable++;
            notifyAll();
        }
    }

    /**
     * Pins a buffer to the specified block, potentially
     * waiting until a buffer becomes available.
     * If no buffer becomes available within a fixed
     * time period, then a {@link NoBufferAvailableException} is thrown.
     * @param blkId a reference to a disk block
     * @return the pinned buffer
     */
    public synchronized Buffer pin(BlockId blkId) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buffer = tryToPin(blkId);
            while (buffer == null && !waitTimeout(timestamp)) {
                wait(MAX_TIME);
                buffer = tryToPin(blkId);
            }
            if (buffer == null)
                throw new NoBufferAvailableException();
            return buffer;
        } catch (InterruptedException e) { // not pin a buffer yet
            throw new NoBufferAvailableException();
        }
    }

    /**
     * Tries to pin a buffer to the specified block.
     * If there is already a buffer assigned to that block
     * then that buffer is used;
     * otherwise, an unpinned buffer from the pool is chosen.
     * Returns a null value if there are no available buffers.
     */
    private Buffer tryToPin(BlockId blkId) {
        Buffer buffer = findExistingBuffer(blkId);
        if (buffer == null) { // no existing buffer with that blkId
            buffer = chooseUnpinnedBuffer();
            if (buffer == null) // all pages are pinned
                return null;
            // buffer: an unpinned pages chosen using Naive strategy
            buffer.assignToBlock(blkId);
        }
        // buffer can be an existing pinned one, or a new
        // unpinned buffer
        if (!buffer.isPinned())
            numAvailable--;
        buffer.pin();
        return buffer;
    }

    private boolean waitTimeout(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }

    private Buffer findExistingBuffer(BlockId blkId) {
        // using inefficient sequential scan
        for (Buffer buffer: bufferPool) {
            BlockId b = buffer.block();
            if (b != null && b.equals(blkId))
                return buffer;
        }
        return null;
    }

    private Buffer chooseUnpinnedBuffer() {
        for (Buffer buffer: bufferPool) {
            if (!buffer.isPinned())
                return buffer;
        }
        return null;
    }
}
