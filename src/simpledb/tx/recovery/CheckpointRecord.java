package simpledb.tx.recovery;

import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class CheckpointRecord implements LogRecord {

    public CheckpointRecord() {
    }

    @Override
    public int op() {
        return CHECKPOINT;
    }

    @Override
    public int txNumber() {
        return -1;
    }

    @Override
    public void undo(Transaction tx) {}

    @Override
    public String toString() {
        return "<CHECKPOINT>";
    }

    public static int writeToLog(LogMgr lm) {
        byte[] rec = new byte[Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, CHECKPOINT);
        return lm.append(rec);
    }
}
