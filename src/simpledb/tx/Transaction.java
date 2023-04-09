package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.concurrency.ConcurrencyMgr;
import simpledb.tx.recovery.RecoveryMgr;

public class Transaction {

    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1; // dummy blockId, for phantom problem prevention
    private FileMgr fm;
    private LogMgr lm;
    private BufferMgr bm;

    private int txNum;
    private RecoveryMgr recoveryMgr;
    private ConcurrencyMgr concurrencyMgr;
    private BufferList bufferList;

    public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm) {
        this.fm = fm;
        this.lm = lm;
        this.bm = bm;
        this.txNum = nextTxNum();
        this.recoveryMgr = new RecoveryMgr(this, txNum, lm, bm);
        this.concurrencyMgr = new ConcurrencyMgr();
        this.bufferList = new BufferList(bm);
    }

    /* lifespan related methods */
    public void commit() {
        recoveryMgr.commit();
        concurrencyMgr.release();
        bufferList.unpinAll();
        System.out.println("Tx " + txNum + " committed.");
    }

    public void rollback() {
        recoveryMgr.rollback();
        concurrencyMgr.release();
        bufferList.unpinAll();
        System.out.println("Tx " + txNum + " rollbacked.");
    }

    /**
     * Go through the log, rolling back all uncommitted transactions.
     * Finally write a quiescent checkpoint to the log.
     * This method is called during system startup, before user txs begins.
     */
    public void recover() {
        recoveryMgr.recover();
    }

    /* methods to access buffer */
    public void pin(BlockId blkId) {
        bufferList.pin(blkId);
    }

    public void unpin(BlockId blkId) {
        bufferList.unpin(blkId);
    }

    public int getInt(BlockId blkId, int offset) {
        concurrencyMgr.sLock(blkId);
        Buffer buf = bufferList.getBuffer(blkId);
        return buf.contents().getInt(offset);
    }

    public String getString(BlockId blkId, int offset) {
        concurrencyMgr.sLock(blkId);
        Buffer buf = bufferList.getBuffer(blkId);
        return buf.contents().getString(offset);
    }

    public void setInt(BlockId blkId, int offset, int val, boolean okToLog) {
        concurrencyMgr.xLock(blkId);
        Buffer buf = bufferList.getBuffer(blkId);
        int lsn = -1;
        if (okToLog)
            lsn = recoveryMgr.setInt(buf, offset, val);

        Page p = buf.contents();
        p.setInt(offset, val);
        buf.setModified(txNum, lsn);
    }

    public void setString(BlockId blkId, int offset, String val, boolean okToLog) {
        concurrencyMgr.xLock(blkId);
        Buffer buf = bufferList.getBuffer(blkId);
        int lsn = -1;
        if (okToLog)
            lsn = recoveryMgr.setString(buf, offset, val);

        Page p = buf.contents();
        p.setString(offset, val);
        buf.setModified(txNum, lsn);
    }

    public int availableBuffs() {
        return bm.available();
    }

    /* methods related to file manager */
    public int size(String filename) {
        BlockId dummyBlk = new BlockId(filename, END_OF_FILE);
        concurrencyMgr.sLock(dummyBlk);
        return fm.lengthInBlocks(filename);
    }

    public BlockId append(String filename) {
        BlockId dummyBlk = new BlockId(filename, END_OF_FILE);
        concurrencyMgr.sLock(dummyBlk);
        return fm.append(filename);
    }

    public int blockSize() {
        return fm.blockSize();
    }

    private static synchronized int nextTxNum() {
        nextTxNum++;
        System.out.println("New transaction: " + nextTxNum);
        return nextTxNum;
    }
}
