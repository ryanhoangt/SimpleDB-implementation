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

    /**
     * A static method to write a checkpoint record to the log.
     * This log record contains the CHECKPOINT operator,
     * and nothing else.
     * @return the LSN of the last log value
     */
    public static int writeToLog(LogMgr lm) {
        byte[] rec = new byte[Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, CHECKPOINT);
        return lm.append(rec);
    }
}
