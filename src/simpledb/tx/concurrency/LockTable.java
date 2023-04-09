package simpledb.tx.concurrency;

import simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

/**
 * Package-private Lock table used by individual ConcurrencyMgr
 * object.
 */
class LockTable {

    private static final long MAX_TIMEOUT = 10000; // 10 seconds
    private Map<BlockId, Integer> locks = new HashMap<>();

    public synchronized void sLock(BlockId blockId) {
        try {
            long startWaitingTime = System.currentTimeMillis();
            while (hasXLock(blockId) && !waitingTimeout(startWaitingTime))
                wait(MAX_TIMEOUT);

            if (hasXLock(blockId))
                throw new LockAbortException();

            int val = getLockVal(blockId);
            locks.put(blockId, val+1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    /**
     * Note: This method is called ONLY after the ConcurrencyMgr
     * has already obtained an sLock, which would ensure no
     * other xLocks are set.
     */
    public synchronized void xLock(BlockId blockId) {
        try {
            long startWaitingTime = System.currentTimeMillis();
            while (hasOtherSLocks(blockId) && !waitingTimeout(startWaitingTime))
                wait(MAX_TIMEOUT);

            if (hasOtherSLocks(blockId))
                throw new LockAbortException();
            locks.put(blockId, -1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(BlockId blockId) {
        int val = getLockVal(blockId);
        if (val > 1)
            locks.put(blockId, val-1);
        else {
            locks.remove(blockId);
            notifyAll();
        }
    }

    private int getLockVal(BlockId blockId) {
        if (!locks.containsKey(blockId))
            return 0;

        int val = locks.get(blockId);
        return val;
    }

    private boolean hasXLock(BlockId blockId) {
        return getLockVal(blockId) < 0;
    }

    private boolean hasOtherSLocks(BlockId blockId) {
        return getLockVal(blockId) > 1;
    }

    private boolean waitingTimeout(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIMEOUT;
    }
}
