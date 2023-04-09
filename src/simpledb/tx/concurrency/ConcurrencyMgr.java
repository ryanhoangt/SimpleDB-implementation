package simpledb.tx.concurrency;

import simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

/**
 * The concurrency manager for the transaction. Each transaction
 * has its own concurrency manager. It keeps track of which locks
 * the transaction currently has, and interact with the global lock
 * table if needed.
 */
public class ConcurrencyMgr {
    // a shared lock table used by all txs
    private static LockTable lockTable = new LockTable();
    private Map<BlockId, String> locks = new HashMap<>();

    public void sLock(BlockId blockId) {
        if (locks.get(blockId) == null) {
            lockTable.sLock(blockId);
            locks.put(blockId, "S");
        }
    }

    public void xLock(BlockId blockId) {
        if (!hasXLock(blockId)) {
            sLock(blockId); // obtain sLock first
            lockTable.xLock(blockId);
            locks.put(blockId, "X");
        }
    }

    /**
     * Release all locks by asking the lock table to unlock each one.
     */
    public void release() {
        for (BlockId blockId: locks.keySet()) {
            lockTable.unlock(blockId);
        }
        locks.clear();
    }

    private boolean hasXLock(BlockId blockId) {
        String lockType = locks.get(blockId);
        return lockType != null && lockType.equals("X");
    }

}
