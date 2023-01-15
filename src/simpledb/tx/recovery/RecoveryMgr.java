package simpledb.tx.recovery;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class RecoveryMgr {

    private Transaction tx;
    private int txnum;
    private LogMgr lm;
    private BufferMgr bm;

    public RecoveryMgr(Transaction tx, int txnum, LogMgr lm, BufferMgr bm) {
        this.tx = tx;
        this.txnum = txnum;
        this.lm = lm;
        this.bm = bm;
        StartRecord.writeToLog(lm, txnum);
    }

    /**
     * Implement using Undo-only strategy: flush modifications then
     * write a commit record to the log, and flushes log to disk.
     */
    public void commit() {
        bm.flushAll(txnum);
        int lsn = CommitRecord.writeToLog(lm, txnum);
        lm.flush(lsn);
    }

    public void rollback() {
        doRollback();
        bm.flushAll(txnum);
        int lsn = RollbackRecord.writeToLog(lm, txnum);
        lm.flush(lsn);
    }

    /**
     * Rollback the transaction, by iterating
     * through the log records until it finds
     * the transaction's START record,
     * calling undo() for each of the transaction's
     * log records.
     */
    private void doRollback() {
        Iterator<byte[]> iter = lm.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.reconstructLogRecord(bytes);
            if (rec.txNumber() == txnum) {
                if (rec.op() == LogRecord.START) return;
                rec.undo(tx);
            }
        }
    }

    public void recover() {
        doRecover();
        bm.flushAll(txnum);
        int lsn = CheckpointRecord.writeToLog(lm);
        lm.flush(lsn);
    }

    /**
     * Do a complete database recovery.
     * The method iterates through the log records.
     * Whenever it finds a log record for an unfinished
     * transaction, it calls undo() on that record.
     * The method stops when it encounters a CHECKPOINT record
     * or the end of the log.
     */
    private void doRecover() {
        Collection<Integer> finishedTxs = new ArrayList<>();
        Iterator<byte[]> iter = lm.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.reconstructLogRecord(bytes);
            if (rec.op() == LogRecord.CHECKPOINT) return;
            if (rec.op() == LogRecord.COMMIT || rec.op() == LogRecord.ROLLBACK)
                finishedTxs.add(rec.txNumber());
            else if (!finishedTxs.contains(rec.txNumber()))
                rec.undo(tx);
        }
    }

    /**
     * Write a setint record to the log and return its lsn.
     * @param buffer the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newval the value to be written
     */
    public int setInt(Buffer buffer, int offset, int newval) {
        int oldval = buffer.contents().getInt(offset);
        BlockId blkId = buffer.block();
        return SetIntRecord.writeToLog(lm, txnum, blkId, offset, oldval);
    }

    /**
     * Write a setstring record to the log and return its lsn.
     * @param buffer the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newval the value to be written
     */
    public int setString(Buffer buffer, int offset, String newval) {
        String oldval = buffer.contents().getString(offset);
        BlockId blkId = buffer.block();
        return SetStringRecord.writeToLog(lm, txnum, blkId, offset, oldval);
    }

}
